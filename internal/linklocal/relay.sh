#!/bin/sh
set -f
IFS= read -r req
req=$(printf '%s' "$req" | tr -d '\r')
cl=0
hdrs=""
while IFS= read -r h; do
  h=$(printf '%s' "$h" | tr -d '\r')
  [ -z "$h" ] && break
  case $(printf '%s' "$h" | tr 'A-Z' 'a-z') in
    content-length:*) cl=$(printf '%s' "${h#*:}" | tr -d ' ') ;;
    connection:*|x-gopherstack-peer:*) continue ;;
  esac
  hdrs="$hdrs$h
"
done
ino=$(readlink /proc/$$/fd/0 | tr -dc 0-9)
a=$(awk -v i="$ino" '$10==i{print $3; exit}' /proc/net/tcp6 /proc/net/tcp)
a=${a%%:*}
[ ${#a} -eq 32 ] && a=$(printf '%s' "$a" | cut -c25-32)
peer=$(printf '%d.%d.%d.%d' "0x$(printf '%s' "$a" | cut -c7-8)" "0x$(printf '%s' "$a" | cut -c5-6)" "0x$(printf '%s' "$a" | cut -c3-4)" "0x$(printf '%s' "$a" | cut -c1-2)")
set -- $req
{
  printf '%s %s%s %s\r\n' "$1" "$LL_PREFIX" "$2" "$3"
  printf '%s' "$hdrs" | while IFS= read -r l; do printf '%s\r\n' "$l"; done
  printf 'X-Gopherstack-Peer: %s\r\nConnection: close\r\n\r\n' "$peer"
  [ "$cl" -gt 0 ] && head -c "$cl"
} | nc -w 30 "$LL_HOST" "$LL_PORT"
