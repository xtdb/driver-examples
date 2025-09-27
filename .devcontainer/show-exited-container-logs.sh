# sudo su
docker ps -a --filter "status=exited" --format "{{.Names}}" | xargs -r -n1 sh -c 'echo -e "\n=== Logs for $1 ==="; docker logs --tail 50 $1' sh

