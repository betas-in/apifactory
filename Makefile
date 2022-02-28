redis:
	docker pull redis:6-alpine
	docker run \
		--name test.redis \
		-d \
		-p 9876:6379 \
		redis:6-alpine \
			redis-server \
			--requirepass 596a96cc7bf9108cd896f33c44aedc8a

redis_rm:
	docker rm -f test.redis

test:
	go test ./... -timeout 15s -race -cover -coverprofile=coverage.out -v
	go tool cover -html=coverage.out -o coverage.html
