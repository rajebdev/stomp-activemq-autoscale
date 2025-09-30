# Extract the current directory name
APP_NAME := $(notdir $(patsubst %/,%,$(CURDIR)))
AFTER_FIRST_CMD = $(filter-out $@,$(MAKECMDGOALS))

# List Real Command
BUILD_CMD = cargo build --release
RUN_CMD = cargo run --release
TEST_CMD = cargo test
LCOV_FILE = target/lcov.info
COVERAGE_CMD = cargo tarpaulin --out Lcov --output-dir target
CLIPPY_CMD = cargo clippy --message-format=json > target/clippy.json
TIDY_CMD = cargo update
VERSION_CMD = git describe --tags
SYNC_CMD = git pull
SETUP_CMD = cp config.example.yaml config.yaml && cp sample.sonar.env sonar.env
DOCKER_UP_CMD = CONTAINER_NAME=$(APP_NAME)-app docker-compose up -d --build app
DOCKER_DOWN_CMD = CONTAINER_NAME=$(APP_NAME)-app docker-compose stop app
DOCKER_DESTROY_CMD = CONTAINER_NAME=$(APP_NAME)-app docker-compose down
DOCKER_LOGS_CMD = docker logs -f $(APP_NAME)-app
DOCKER_STATS_CMD = docker stats $(APP_NAME)-app
SONARQUBE_CMD = docker run --env-file sonar.env --rm -v .:/usr/src -w /usr/src sonarsource/sonar-scanner-cli

.PHONY: build run test coverage clippy tidy version sync setup up down destroy logs stats sonar normalize

build:
	@$(BUILD_CMD)

run:
	@$(RUN_CMD)

test:
	@$(TEST_CMD)

coverage:
	@$(COVERAGE_CMD)

clippy:
	@$(CLIPPY_CMD)

tidy:
	@$(TIDY_CMD)

version:
	@$(VERSION_CMD)

sync:
	@$(SYNC_CMD)

setup:
	@$(SETUP_CMD)

up:
	@$(DOCKER_UP_CMD)

down:
	@$(DOCKER_DOWN_CMD)

destroy:
	@$(DOCKER_DESTROY_CMD)

logs:
	@$(DOCKER_LOGS_CMD)

stats:
	@$(DOCKER_STATS_CMD)

sonar: #clippy coverage normalize
	@$(SONARQUBE_CMD)

normalize:
	@sed -E -i 's~^SF:.*[/\\]src[/\\]~SF:src/~g' target/lcov.info
	@awk '{sub(/^DA:DA:/,"DA:"); \
		if($$0~/^FNDA:[0-9]+,/){split($$0,a,":"); split(a[2],b,","); if(b[1]+0>1000000){$$0="FNDA:1," substr(a[2],index(a[2],",")+1)}} \
		else if($$0~/^DA:[0-9]+,[0-9]+(,[0-9A-Fa-f]+)?$$/){split($$0,a,":"); split(a[2],b,","); if(b[2]+0>1000000){$$0="DA:" b[1] ",1" (b[3] ? "," b[3] : "")}}; \
		print}' target/lcov.info > target/lcov_temp.info && mv target/lcov.info target/lcov_original.info && mv target/lcov_temp.info target/lcov.info