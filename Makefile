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

.PHONY: build run test coverage cov-fast watch-cov clippy tidy version sync setup up down destroy logs stats sonar normalize

build:
	@$(BUILD_CMD)

run:
	@$(RUN_CMD)

test:
	@$(TEST_CMD)

coverage:
	@$(COVERAGE_CMD)

cov-fast:
	@cargo llvm-cov nextest --html
	@echo ""
	@echo "=========================================================================================================================="
	@echo "File                           | Function              | Line                  | Region                | Branch"
	@echo "=========================================================================================================================="
	@cat target/llvm-cov/html/index.html \
	| sed 's/<\/tr>/\n/g' \
	| grep "light-row" \
	| while IFS= read -r line; do \
		file=$$(echo "$$line" | grep -o '>[^<]*\.rs<' | sed 's/>//; s/<//' | head -1); \
		if [ -z "$$file" ]; then file=$$(echo "$$line" | grep -o '>Totals<' | sed 's/>//; s/<//'); fi; \
		allpre=$$(echo "$$line" | grep -o '<pre>[^<]*</pre>' | sed 's/<pre>//; s/<\/pre>//'); \
		func=$$(echo "$$allpre" | sed -n '1p'); \
		linec=$$(echo "$$allpre" | sed -n '2p'); \
		region=$$(echo "$$allpre" | sed -n '3p'); \
		branch=$$(echo "$$allpre" | sed -n '4p'); \
		if [ "$$file" = "Totals" ]; then \
			echo "--------------------------------------------------------------------------------------------------------------------------"; \
			func=$$(echo "$$allpre" | sed -n '2p'); \
			linec=$$(echo "$$allpre" | sed -n '3p'); \
			region=$$(echo "$$allpre" | sed -n '4p'); \
			branch=$$(echo "$$allpre" | sed -n '5p'); \
		fi; \
		if [ -n "$$file" ]; then \
			printf "%-30s | %-21s | %-21s | %-21s | %s\n" "$$file" "$$func" "$$linec" "$$region" "$$branch"; \
		fi; \
	done
	@echo "=========================================================================================================================="

watch-cov:
	@cargo watch -q -w src -w Cargo.toml -s "make -s cov-fast"

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

sonar: clippy coverage normalize
	@$(SONARQUBE_CMD)

normalize:
	@sed -E -i 's~^SF:.*[/\\]src[/\\]~SF:src/~g' target/lcov.info
	@awk '{sub(/^DA:DA:/,"DA:"); \
		if($$0~/^FNDA:[0-9]+,/){split($$0,a,":"); split(a[2],b,","); if(b[1]+0>1000000){$$0="FNDA:1," substr(a[2],index(a[2],",")+1)}} \
		else if($$0~/^DA:[0-9]+,[0-9]+(,[0-9A-Fa-f]+)?$$/){split($$0,a,":"); split(a[2],b,","); if(b[2]+0>1000000){$$0="DA:" b[1] ",1" (b[3] ? "," b[3] : "")}}; \
		print}' target/lcov.info > target/lcov_temp.info && mv target/lcov.info target/lcov_original.info && mv target/lcov_temp.info target/lcov.info