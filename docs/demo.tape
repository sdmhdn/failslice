# FailSlice demo GIF (ADR 0013). Rendered by .github/workflows/demo-gif.yml with vhs,
# with FailSlice and pytest preinstalled from the built wheels. Recorded in real time:
# nothing is sped up or cut. Regenerate by running that workflow (Actions -> demo GIF).

Output docs/assets/demo.gif

Set Shell bash
Set FontSize 16
Set Width 1100
Set Height 640
Set Padding 16
Set Framerate 10
Set TypingSpeed 35ms
Set Theme "Catppuccin Mocha"
Set WaitTimeout 90s

Hide
Type "cd $(mktemp -d) && clear"
Enter
Show

Type "# real time, not sped up. A total is wrong on 26,001 rows in 3 tables."
Enter
Sleep 1.5s
Type "failslice demo shop && cd shop"
Enter
Wait
Sleep 2s

Type "failslice reduce --out run"
Enter
Wait
Sleep 3s

Type "failslice explain run | head -n 12"
Enter
Wait
Sleep 4s

Type "pytest -q run/pytest"
Enter
Wait
Sleep 4s
