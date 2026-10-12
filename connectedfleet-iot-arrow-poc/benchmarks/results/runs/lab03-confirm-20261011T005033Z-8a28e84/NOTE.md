# Stopped before its first case: the host never became quiet

This run (8a28e84) waited from 00:50Z to 01:48Z for 60 quiet seconds while a native soak test used 9-14 cores (load average 50-72). No case ran; nothing is cited. It was restarted with a 12-hour wait limit per case (the 2-hour limit would have turned a long wait into an error case).
