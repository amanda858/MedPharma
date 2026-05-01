import subprocess
r = subprocess.run(["git","log","--oneline","-5"], cwd="/workspaces/CVOPro", capture_output=True, text=True)
print(r.stdout)
