use std::env;
use std::os::unix::prelude::CommandExt;
use std::process::{Child, Command, Stdio};

use nix::sys::signal;

/**
Sigforwarder catches signals and forwards them to its grandchildren processes

Build sigforwarder with

    cargo build --package skupper-router-scripts --bin sigforwarder
    # this outputs binary to `target/debug/sigforwarder`

To use sigforwarder, configure CMake with

    -DQDROUTERD_RUNNER="/absolute/path/to/sigforwarder rr record --print-trace-dir=1"

The parameter to rr causes it to print the trace name for each given router to ctest log.
Same output can be found in files in the build directory, such as `build/tests/system_test.dir/system_tests_autolinks/AutoLinkRetryTest/setUpClass/A-2.out`

```
$ ctest -I 15,15 -VV
[...]
15: Router A output file:
15: >>>>
15: /home/jdanek/.local/share/rr/skrouterd-22
15:
15: <<<<
```

## Motivation

The router tests offer the `-DQDROUTERD_RUNNER` CMake option.
This allows interposing any program of our choice between the Python test process and the skrouterd instances running under test.
For example, we may use the rr (record-reply debugger from Mozilla) as runner and record router execution this way.

The tests send SIGTERM to the immediate child when it is time to stop the router.
This is problematic with rr, because in response to SIGTERM, rr quits itself and does not record shutdown of its child (skrouterd).

Therefore, add sigforwarder on top, so the process tree looks like this

```
      Python (test process)
         |
    sigforwarder (this program)
         |
      rr record
         |
     skrouterd (the Software Under Test)
```

Now, sigforwarder intercepts the SIGTERM and delivers it to skrouterd (actually, to all grandchildren processes we might have).

Note that rr option `--continue-through-signal` does something different than sigforwarder and does not help here.
*/

/// Only one child process; we are single-threaded, no need to protect it
static mut P: Option<Child> = None;

/// This will be run in the child before doing exec syscall; to be extra sure
/// we don't leave any zombies around. Because Linux has this feature.
fn pre_exec() -> std::io::Result<()> {
    // https://stackoverflow.com/questions/284325/how-to-make-child-process-die-after-parent-exits
    // https://stackoverflow.com/questions/284325/how-to-make-child-process-die-after-parent-exits
    // https://github.com/nix-rust/nix/issues/601
    unsafe {
        let ret = libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGHUP);
        if ret != 0 {
            libc::perror("Failed to run prctl\0".as_ptr() as *const libc::c_char);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Failure in prctl"));
        }
        return Ok(());
    }
}

/// Forward the signal we got to the grandchildren
extern fn handle_signal(signal: i32) {
    let signal = nix::sys::signal::Signal::try_from(signal).unwrap();
    log::trace!("sigforwarder got signal: {}", signal);

    // forward to grandchildren of process
    unsafe {
        if let Some(p) = &P {
            log::trace!("We have child, lets do things to it");
            for prc in procfs::process::all_processes().unwrap() {
                if prc.stat.ppid as u32 == p.id() {
                    log::trace!("Grandchild pid: {}, sending signal to it", prc.pid);
                    nix::sys::signal::kill(nix::unistd::Pid::from_raw(prc.pid), signal)
                        .expect("Failed to send signal to grandchild");
                }
            }
        }
    }
}


fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("At least one argument is required");
    }
    let program = &args[1];
    let program_args = &args[2..];

    let code = sigforwarder(program, program_args);
    std::process::exit(code);
}

// https://stackoverflow.com/questions/41179659/convert-vecstring-into-a-slice-of-str-in-rust
fn sigforwarder<T: AsRef<std::ffi::OsStr>>(program: &str, program_args: &[T]) -> i32 {
    // oh, all the unsafe things we do to children
    unsafe {
        P = Some(Command::new(program)
            .pre_exec(pre_exec)
            .args(program_args)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("failed to execute subprocess"));
    }

    // first start the child, then install signal handler, so that we cannot
    // be asked to handle a signal when child is not yet present
    let sig_action = signal::SigAction::new(
        signal::SigHandler::Handler(handle_signal),
        signal::SaFlags::empty(),
        signal::SigSet::empty());
    for s in [signal::SIGHUP, signal::SIGQUIT, signal::SIGTERM, signal::SIGINT] {
        unsafe {
            signal::sigaction(s, &sig_action)
                .expect("Calling sigaction failed");
        }
    }

    unsafe {
        if let Some(p) = &mut P {
            let status = p.wait()
                .expect("Waiting for subprocess failed");

            // rr will propagate exit code from skrouterd, so we only need to propagate from rr
            let code = status.code().expect("Subprocess did not provide exit code");
            return code;
        }
    }
    return 1;
}

#[cfg(test)]
mod sigforwarder_tests {
    #[test]
    fn test_run_child_process_propagate_exit_code() {
        let res = crate::sigforwarder("/bin/sh", ["-c", "exit 42"].as_slice());
        assert_eq!(res, 42);
    }

    #[test]
    fn test_run_child_process_parent_kill_is_ignored() {
        let res = crate::sigforwarder("/usr/bin/bash", ["-c", r#"
        kill -SIGTERM $PPID
        "#].as_slice());
        assert_eq!(res, 0);
    }

    #[test]
    fn test_run_child_process_grandchild_gets_the_kill() {
        // be careful to not leave `sleep infinity` behind, http://mywiki.wooledge.org/SignalTrap#When_is_the_signal_handled.3F
        let res = crate::sigforwarder("/usr/bin/bash", ["-c", r#"
        trap "echo 'WRONG: child has trapped'; exit 11" TERM

        bash -c $'pid=baf; trap \'[[ -v pid ]] && kill $pid; exit 22\' TERM; sleep infinity & pid=$!; wait $pid' &
        grandchild=$!

        kill -SIGTERM $PPID
        wait $grandchild
        exit $?
        "#].as_slice());
        assert_eq!(res, 22);
    }
}
