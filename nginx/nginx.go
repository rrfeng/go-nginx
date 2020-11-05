package nginx

import (
	"context"
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
	prefix     string
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
	var binfile, configfile, prefix string
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
			prefix = args[i+1]
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
		prefix:     prefix,
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

func (n *Nginx) Prefix() string {
	return n.prefix
}

func (n *Nginx) BinaryFile() string {
	return n.binfile
}

func (n *Nginx) ConfigFile() string {
	return n.configfile
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

	err = waitWorkersGoingShutting(workers, ctx)
	return
}

func (n *Nginx) Upgrade(newbinfile string, healthCheckFunc func() error, timeout time.Duration) (ngx *Nginx, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var phase int

	// 根据最后执行的 phase 进行回滚，若遇到回滚错误则不再执行后续回滚步骤，直接返回错误
	defer func() {
		if err == nil {
			return
		}

		if phase > upgradePhaseStopOldWorker {
			rberr := n.startWorkers(ctx)
			err = errors.WithMessage(err, "rollback(try start old worker) fail: "+rberr.Error())
			return
		}

		if phase > upgradePhaseStartNewMaster {
			rberr := ngx.quit(ctx)
			err = errors.WithMessage(err, "rollback(try stop new master) fail: "+rberr.Error())
			return
		}

		if phase > upgradePhaseCopyNewBin {
			rberr := os.Rename(n.binfile, newbinfile)
			err = errors.WithMessage(err, "rollback(try remove new bin) fail: "+rberr.Error())
			return
		}

		if phase > upgradePhaseBackupOldBin {
			rberr := os.Rename(n.binfile+".old", n.binfile)
			err = errors.WithMessage(err, "rollback(try restore old bin) fail: "+rberr.Error())
			return
		}

	}()

	phase = upgradePhaseBackupOldBin
	err = os.Rename(n.binfile, n.binfile+".old")
	if err != nil {
		err = errors.WithMessage(err, "phase backup old bin")
		return
	}

	phase = upgradePhaseCopyNewBin
	err = os.Rename(newbinfile, n.binfile)
	if err != nil {
		err = errors.WithMessage(err, "phase copy new bin")
		return
	}

	phase = upgradePhaseStartNewMaster
	ngx, err = n.forkNewMaster(ctx)
	if err != nil {
		err = errors.WithMessage(err, "phase start new master")
		return
	}

	phase = upgradePhaseStopOldWorker
	err = n.closeWorkers(ctx)
	if err != nil {
		err = errors.WithMessage(err, "phase stop old worker")
		return
	}

	phase = upgradePhaseHealthCheck
	err = healthCheckFunc()
	if err != nil {
		err = errors.WithMessage(err, "phase healthcheck")
		return
	}

	phase = upgradePhaseStopOldMaster
	err = n.quit(ctx)
	if err != nil {
		err = errors.WithMessage(err, "phase stop old master")
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
	return waitNewWorkersStarted(n.master, ctx)
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

	err = waitWorkersGoingShutting(workers, ctx)
	return
}

func waitNewWorkersStarted(parent *process.Process, ctx context.Context) (err error) {
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

func waitWorkersGoingShutting(workers []*process.Process, ctx context.Context) (err error) {

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
				if cmdline == "" || strings.Contains(cmdline, "shutting down") {
					// 如果进程已经被关闭，那么 cmdline 就是空的
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
