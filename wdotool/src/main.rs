use clap::Parser;

use wdotool::{
    cli::{Command, ServerCommand},
    diag, server, Cli, DispatchCtx, ExitCode,
};
use wdotool_core::detector::{self, BackendKind, Environment};
use wdotool_core::WdoError;

#[cfg(feature = "recorder")]
use wdotool::record;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    wdotool::init_tracing(cli.verbose);

    // Diag has to short-circuit before detector::build so the probes
    // never touch the portal session (which would pop a consent dialog
    // for libei users, exactly the surprise diag is meant to remove).
    if let Command::Diag { json, copy } = cli.command {
        let format = if json {
            diag::DiagFormat::Json
        } else {
            diag::DiagFormat::Markdown
        };
        return diag::run(format, copy);
    }

    // Route clients before any detection/build, including status and stop.
    if let Command::Server { command } = cli.command {
        anyhow::ensure!(
            !cli.remote,
            "--remote cannot be combined with server management"
        );
        let path = server::socket::path(cli.socket)?;
        match command {
            ServerCommand::Launch => {
                server::launch(&path, parse_backend(cli.backend.as_deref())?).await?
            }
            command => {
                anyhow::ensure!(
                    cli.backend.is_none(),
                    "--backend only applies to server launch"
                );
                let request = match command {
                    ServerCommand::Status => server::protocol::Request::Status,
                    ServerCommand::Stop => server::protocol::Request::Stop,
                    ServerCommand::Launch => unreachable!(),
                };
                let exit = server::print_response(server::request(&path, request).await?)?;
                if !exit.is_success() {
                    std::process::exit(exit.0);
                }
            }
        }
        return Ok(());
    }
    if cli.remote {
        let request = server::protocol::prepare(cli.command)?;
        let path = server::socket::path(cli.socket)?;
        let exit = server::print_response(server::request(&path, request).await?)?;
        if !exit.is_success() {
            std::process::exit(exit.0);
        }
        return Ok(());
    }
    anyhow::ensure!(
        cli.socket.is_none(),
        "--socket requires --remote or server management"
    );

    // Record short-circuits before detector::build for the same
    // reason Diag does: the recorder owns its own portal session
    // (libei in receiver mode) and doesn't need a sender Backend
    // to bootstrap.
    #[cfg(feature = "recorder")]
    if let Command::Record {
        output,
        max_duration,
        backend,
    } = cli.command
    {
        return record::run(output, max_duration, backend).await;
    }

    let env = Environment::detect();

    // Prime short-circuits the dispatch loop so it can hold the
    // wlr-protocols backend alive in the foreground until a signal
    // arrives. We always pick wlr-protocols regardless of cli.backend
    // because that's the only backend whose devices live across calls.
    if let Command::Prime = cli.command {
        let backend = detector::build(&env, Some(BackendKind::WlrProtocols)).await?;
        // Stdout is the readiness signal for any test harness (or
        // human) waiting on us. Flush so the `ready` line lands
        // before this process blocks on the signal handler.
        println!("ready");
        std::io::Write::flush(&mut std::io::stdout()).ok();
        // Block until Ctrl-C or SIGTERM. Both end the wait the same
        // way; the backend (and its virtual devices) gets dropped
        // when this function returns, which cleanly releases the
        // devices in the compositor.
        tokio::signal::ctrl_c().await?;
        drop(backend);
        return Ok(());
    }

    let forced = parse_backend(cli.backend.as_deref())?;

    let backend = detector::build(&env, forced).await?;

    let exit = {
        let mut stdout = std::io::stdout().lock();
        let mut stderr = std::io::stderr().lock();
        let mut ctx = DispatchCtx {
            backend: &*backend,
            env: &env,
            stdout: &mut stdout,
            stderr: &mut stderr,
        };
        wdotool::dispatch(&mut ctx, cli.command).await?
    };
    if exit != ExitCode::SUCCESS {
        std::process::exit(exit.0);
    }
    Ok(())
}

/// Share backend spelling validation between direct mode and server launch.
fn parse_backend(value: Option<&str>) -> anyhow::Result<Option<BackendKind>> {
    Ok(match value {
        Some(s) => Some(BackendKind::parse(s).ok_or_else(|| {
            WdoError::InvalidArg(format!(
                "unknown backend '{s}' (expected libei, wlr-protocols, kde, gnome, uinput)"
            ))
        })?),
        None => None,
    })
}
