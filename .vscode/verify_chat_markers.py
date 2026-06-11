"""Quick presence-check of every chat/modules/access marker we just added."""
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HTML = os.path.join(ROOT, "app", "templates", "client_hub.html")

html = open(HTML).read()
markers = [
    "panel-chat", "chatRoomModal", "chatMembersModal", "chatRoomActions",
    "chatComposer", "applyModuleVisibility", "data-module=",
    "clientAccessModal", "clientModulesModal", "fCModulesWrap",
    "fCAccessWrap", "navChatBadge", "navChatBadgeClient",
    "CHAT.activeRoomId", "openChatRoomModal", "chatSaveRoom",
    "chatSendMessage", "openClientAccessModal", "openClientModulesModal",
    "refreshChatBadge", "fRoomMember", "fRoomClient",
]
print(f"total size: {len(html):,} chars\n")
ok = True
for m in markers:
    n = html.count(m)
    mark = "✓" if n else "✗"
    if not n:
        ok = False
    print(f"{mark} {m}: ×{n}")
print()
print("ALL MARKERS PRESENT" if ok else "*** MISSING MARKERS ***")
