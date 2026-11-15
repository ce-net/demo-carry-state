//! End to end: real processes A and B, clients under load, several moves, the audit must hold.

use std::process::Command;

#[test]
fn moves_under_load_lose_and_duplicate_nothing() {
    let out = Command::new(env!("CARGO_BIN_EXE_carry-state"))
        .args(["prove", "--moves", "6", "--clients", "4", "--rate", "100", "--settle-ms", "150"])
        .output()
        .expect("run carry-state");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "prove failed:\n{stdout}\n{}", String::from_utf8_lossy(&out.stderr));
    assert!(stdout.contains("all 6 moves: no write lost, none duplicated"), "{stdout}");
    assert!(stdout.contains("old holder now answers: \"moved, epoch 2\""), "{stdout}");
    assert!(!stdout.contains("MISMATCH"), "{stdout}");
}
