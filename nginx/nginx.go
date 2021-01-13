package nginx

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/process"
)

type Nginx struct {
	master     *process.Process
	binfile    string
	workdir    string
	configfile string
}

const (
	upgradePhaseBackupOldBin = iota
	upgradePhaseCopyNewBin
	upgradePhaseStartNewMaster
	upgradePhaseStopOldWorker
	upgradePhaseHealthCheck
	upgradePhaseStopOldMaster
)

func NewNginxByPid(pid int32) (ngx *Nginx, err error) {
	p, err := process.NewProcess(pid)
	if err != nil {
		err = errors.WithMessagef(err, "cannot get process of pid %d", pid)
		return
	}

	args, err := p.CmdlineSlice()
	if err != nil {
		err = errors.WithMessagef(err, "cannot get cmdline of pid %d", pid)
		return
	}

	// nginx: master process /path/to/nginx -p ... -c ...
	var binfile, configfile, workdir string
	cmdLength := len(args)
	if cmdLength >= 4 &&
		args[0] == "nginx:" &&
		args[1] == "master" &&
		args[3] == "process" {
		binfile = args[4]
	} else {
		err = errors.New("invalid master process cmdline: " + strings.Join(args, " "))
		return
	}

	for i := 4; i < cmdLength; i++ {
		if args[i] == "-p" {
			workdir = args[i+1]
			i++
		}

		if args[i] == "-c" {
			configfile = args[i+1]
			i++
		}
	}

	ngx = &Nginx{
		master:     p,
		binfile:    binfile,
		workdir:    workdir,
		configfile: configfile,
	}

	return
}

func NewNginxByPidFile(pidfile string) (ngx *Nginx, err error) {
	pidstr, err := ioutil.ReadFile(pidfile)
	if err != nil {
		return
	}
	pid, err := strconv.Atoi(string(pidstr))
	if err != nil {
		err = errors.WithMessagef(err, "cannot parse pid %s", pidstr)
		return
	}

	return NewNginxByPid(int32(pid))
}

func (n *Nginx) Pid() int32 {
	return n.master.Pid
}

func (n *Nginx) Workdir() string {
	return n.workdir
}

func (n *Nginx) BinaryFile() string {
	return n.binfile
}

func (n *Nginx) ConfigFile() string {
	return n.configfile
}

func (n *Nginx) String() string {
	return fmt.Sprintf("Pid: %d, Binary: %s, Prefix: %s, Config: %s",
		n.master.Pid, n.binfile, n.workdir, n.configfile)
}

func (n *Nginx) Reload(timeout time.Duration) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	workers, err := n.master.ChildrenWithContext(ctx)
	if err != nil {
		err = errors.WithMessage(err, "cannot find workers")
		return
	}

	err = n.master.SendSignalWithContext(ctx, syscall.SIGUSR1)
	if err != nil {
		err = errors.WithMessage(err, "cannot send reload signal")
		return
	}

	err = waitWorkersGoingShutting(ctx, workers)
	return
}

func (n *Nginx) Upgrade(newbinfile string, healthCheckFunc func() error, timeout time.Duration) (ngx *Nginx, upgradeErr, rollbackErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var phase int

	// 根据最后执行的 phase 进行回滚，若遇到回滚错误则不再执行后续回滚步骤，直接返回错误
	defer func() {
		if upgradeErr == nil {
			return
		}

		if phase > upgradePhaseStopOldWorker {
			if rollbackErr = n.startWorkers(ctx); rollbackErr != nil {
				rollbackErr = errors.WithMessage(rollbackErr, "rollback(try start old worker) fail")
				return
			}
		}

		if phase > upgradePhaseStartNewMaster {
			if rollbackErr = ngx.quit(ctx); rollbackErr != nil {
				rollbackErr = errors.WithMessage(rollbackErr, "rollback(try stop new master) fail")
				return
			}
		}

		if phase > upgradePhaseCopyNewBin {
			if rollbackErr = os.Rename(n.binfile, newbinfile); rollbackErr != nil {
				rollbackErr = errors.WithMessage(rollbackErr, "rollback(try remove new bin) fail")
				return
			}
		}

		if phase > upgradePhaseBackupOldBin {
			if rollbackErr = os.Rename(n.binfile+".old", n.binfile); rollbackErr != nil {
				rollbackErr = errors.WithMessage(rollbackErr, "rollback(try restore old bin) fail")
				return
			}
		}

	}()

	phase = upgradePhaseBackupOldBin
	upgradeErr = os.Rename(n.binfile, n.binfile+".old")
	if upgradeErr != nil {
		upgradeErr = errors.WithMessage(upgradeErr, "phase backup old bin failed")
		return
	}

	phase = upgradePhaseCopyNewBin
	upgradeErr = os.Rename(newbinfile, n.binfile)
	if upgradeErr != nil {
		upgradeErr = errors.WithMessage(upgradeErr, "phase copy new bin failed")
		return
	}

	phase = upgradePhaseStartNewMaster
	ngx, upgradeErr = n.forkNewMaster(ctx)
	if upgradeErr != nil {
		upgradeErr = errors.WithMessage(upgradeErr, "phase start new master failed")
		return
	}

	phase = upgradePhaseStopOldWorker
	upgradeErr = n.closeWorkers(ctx)
	if upgradeErr != nil {
		upgradeErr = errors.WithMessage(upgradeErr, "phase stop old worker failed")
		return
	}

	phase = upgradePhaseHealthCheck
	upgradeErr = healthCheckFunc()
	if upgradeErr != nil {
		upgradeErr = errors.WithMessage(upgradeErr, "phase healthcheck failed")
		return
	}

	phase = upgradePhaseStopOldMaster
	upgradeErr = n.quit(ctx)
	if upgradeErr != nil {
		upgradeErr = errors.WithMessage(upgradeErr, "phase stop old master failed")
	}
	return
}

func (n *Nginx) forkNewMaster(ctx context.Context) (ngx *Nginx, err error) {
	err = n.master.SendSignalWithContext(ctx, syscall.SIGUSR2)
	if err != nil {
		err = errors.WithMessagef(err, "cannot send SIGUSR2 to master pid %d", n.master.Pid)
		return
	}

	// 等待新的 master 和 workers 进程出现
	var children []*process.Process
	for {
		select {
		case <-ctx.Done():
			err = errors.New("start new master timeout")
			return
		default:

			if ngx == nil {
				children, err = n.master.ChildrenWithContext(ctx)
				if err != nil {
					err = errors.WithMessage(err, "cannot get child processes")
					return
				}

				for _, worker := range children {
					cmdline, _ := worker.CmdlineWithContext(ctx)
					if strings.Contains(cmdline, "nginx master") {
						ngx, err = NewNginxByPid(worker.Pid)
						if err != nil {
							err = errors.WithMessagef(err, "get nginx from new master pid %d", worker.Pid)
							return
						}

						break
					}
				}

			} else {
				children, err = ngx.master.ChildrenWithContext(ctx)
				if err != nil {
					err = errors.WithMessage(err, "cannot start new workers")
					return
				}

				if len(children) > 0 {
					return
				}
			}
		}
		time.Sleep(time.Millisecond * 200)
	}
}

func (n *Nginx) quit(ctx context.Context) error {
	return n.master.SendSignalWithContext(ctx, syscall.SIGQUIT)
}

func (n *Nginx) startWorkers(ctx context.Context) (err error) {
	err = n.master.SendSignalWithContext(ctx, syscall.SIGHUP)
	if err != nil {
		err = errors.WithMessagef(err, "canot send SIGHUP to master pid %d", n.master.Pid)
		return
	}
	return waitNewWorkersStarted(ctx, n.master)
}

func (n *Nginx) closeWorkers(ctx context.Context) (err error) {
	workers, err := n.master.ChildrenWithContext(ctx)
	if err != nil {
		err = errors.WithMessage(err, "cannot find workers")
		return
	}

	err = n.master.SendSignalWithContext(ctx, syscall.SIGWINCH)
	if err != nil {
		return errors.WithMessagef(err, "cannot send SIGWINCH to pid %d", n.master.Pid)
	}

	err = waitWorkersGoingShutting(ctx, workers)
	return
}

func waitNewWorkersStarted(ctx context.Context, parent *process.Process) (err error) {
	var children []*process.Process
	for {
		select {
		case <-ctx.Done():
			err = errors.New("start new workers timeout")
			return
		default:
			children, err = parent.ChildrenWithContext(ctx)
			if err != nil {
				err = errors.WithMessage(err, "start workers failed")
				return
			}

			if len(children) > 0 {
				return
			}
		}
		time.Sleep(time.Millisecond * 200)
	}
}

func waitWorkersGoingShutting(ctx context.Context, workers []*process.Process) (err error) {

	checked := map[int32]bool{}
	for {
		select {
		case <-ctx.Done():
			err = errors.New("waiting all workers going shutting down timeout")
			return
		default:
			ok := true
			for _, w := range workers {
				if checked[w.Pid] {
					continue
				}

				cmdline, _ := w.Cmdline()
				// 如果进程已经被关闭，那么 cmdline 就是空的
				if cmdline == "" || strings.Contains(cmdline, "shutting down") {
					checked[w.Pid] = true
					continue
				}

				// 只要有一个进程不是 shutting down 状态，就继续等待下个循环
				ok = false
				break
			}

			// 所有 worker 要么进入 shutting down 状态，要么已经消失
			if ok {
				err = nil
				return
			}
		}
		time.Sleep(time.Millisecond * 200)
	}
}
