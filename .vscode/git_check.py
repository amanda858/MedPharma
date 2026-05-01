import subprocess, os
os.chdir("/workspaces/CVOPro")
print("=== git status ===")
print(subprocess.run(["git","status","--short"], capture_output=True, text=True).stdout)
print("=== git check-ignore ===")
print(subprocess.run(["git","check-ignore","-v","output/labs_routed_full.csv","output/FINAL_apollo_upload.csv"], capture_output=True, text=True).stdout)
print(subprocess.run(["git","check-ignore","-v","output/labs_routed_full.csv","output/FINAL_apollo_upload.csv"], capture_output=True, text=True).stderr)
print("=== current log ===")
print(subprocess.run(["git","log","--oneline","-5"], capture_output=True, text=True).stdout)
