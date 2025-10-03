# Extract the current directory name
APP_NAME := $(notdir $(patsubst %/,%,$(CURDIR)))
AFTER_FIRST_CMD = $(filter-out $@,$(MAKECMDGOALS))

# List Real Command
BUILD_CMD = cargo build --release
RUN_CMD = cargo run --release
TEST_CMD = cargo test
LCOV_FILE = target/lcov.info
COVERAGE_CMD = cargo llvm-cov nextest --lcov --output-path target/lcov.info --workspace --all-features
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
SONARQUBE_CMD = docker run --env-file sonar.env --rm -v .:/usr/src -w /usr/src -v .scannerwork:/opt/sonar-scanner/.scannerwork -v .sonar_cache:/opt/sonar-scanner/.sonar/cache sonarsource/sonar-scanner-cli

.PHONY: build run test coverage cov-fast watch-cov clippy tidy version sync setup up down destroy logs stats sonar normalize filter-test-lines coverage-summary

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

sonar: clippy coverage-summary
	@$(SONARQUBE_CMD)

normalize:
	@sed -E -i 's~^SF:.*[/\\]src[/\\]~SF:src/~g' target/lcov.info
	@awk '{sub(/^DA:DA:/,"DA:"); \
		if($$0~/^FNDA:[0-9]+,/){split($$0,a,":"); split(a[2],b,","); if(b[1]+0>1000000){$$0="FNDA:1," substr(a[2],index(a[2],",")+1)}} \
		else if($$0~/^DA:[0-9]+,[0-9]+(,[0-9A-Fa-f]+)?$$/){split($$0,a,":"); split(a[2],b,","); if(b[2]+0>1000000){$$0="DA:" b[1] ",1" (b[3] ? "," b[3] : "")}}; \
		print}' target/lcov.info > target/lcov_temp.info && mv target/lcov.info target/lcov_original.info && mv target/lcov_temp.info target/lcov_normal.info

filter-test-lines:
	@echo "Filtering test coverage from target/lcov_normal.info..."
	@awk 'BEGIN { \
		curr_file = ""; test_line = 0; written = 0; filtered = 0; \
	} \
	/^SF:/ { \
		curr_file = substr($$0, 4); \
		test_line = 0; \
		if (curr_file != "") { \
			line_num = 0; \
			while ((getline line < curr_file) > 0) { \
				line_num++; \
				if (line ~ /#\[cfg\(test\)\]/) { \
					if ((getline next_line < curr_file) > 0) { \
						line_num++; \
						if (next_line ~ /^\s*mod\s+[A-Za-z0-9_]*tests/) { \
							test_line = line_num; \
							break; \
						} \
					} \
				} \
			} \
			close(curr_file); \
		} \
		if (test_line > 0) { \
			print "[INFO] Found test at line " test_line " in " curr_file > "/dev/stderr"; \
		} else { \
			print "[INFO] No test found in " curr_file > "/dev/stderr"; \
		} \
		print $$0; written++; next; \
	} \
	/^FN:/ { \
		split($$0, p, ","); split(p[1], f, ":"); ln = f[2] + 0; \
		if (test_line > 0 && ln >= test_line) { filtered++; next; } \
	} \
	/^DA:/ { \
		split($$0, p, ","); split(p[1], d, ":"); ln = d[2] + 0; \
		if (test_line > 0 && ln >= test_line) { filtered++; next; } \
	} \
	/^FNDA:/ { \
		if (tolower($$0) ~ /test/) { filtered++; next; } \
	} \
	{ print $$0; written++; } \
	END { \
		print "" > "/dev/stderr"; \
		print "Summary: written=" written ", filtered=" filtered > "/dev/stderr"; \
		print "✓ Done! Output: target/lcov.info" > "/dev/stderr"; \
	}' target/lcov_normal.info > target/lcov.info

coverage-summary: coverage normalize filter-test-lines
	@echo ">>> Coverage Summary (from target/lcov.info)"
	@awk -F, '\
		/^SF:/   { file=$$0; sub(/^SF:/,"",file) } \
		/^DA:/   { l_total[file]++; if ($$2 > 0) l_cov[file]++; lt++; if ($$2 > 0) lct++ } \
		/^FNDA:/ { f_total[file]++; if ($$2 > 0) f_cov[file]++; ft++; if ($$2 > 0) fct++ } \
		END { \
			printf "\n| %-40s | %20s | %20s |\n", "File", "Line Coverage", "Function Coverage"; \
			printf "|%s|%s|%s|\n", "------------------------------------------", "----------------------", "----------------------"; \
			for (f in l_total) { \
				lp = (l_total[f] ? (l_cov[f]/l_total[f])*100 : 0); \
				fp = (f_total[f] ? (f_cov[f]/f_total[f])*100 : 0); \
				printf "| %-40s | %7.2f%% (%4d/%4d) | %7.2f%% (%4d/%4d) |\n", \
					f, lp, l_cov[f], l_total[f], fp, f_cov[f], f_total[f]; \
			} \
			printf "|%s|%s|%s|\n", "------------------------------------------", "----------------------", "----------------------"; \
			printf "| %-40s | %7.2f%% (%4d/%4d) | %7.2f%% (%4d/%4d) |\n", \
				"TOTAL", (lt ? (lct/lt)*100 : 0), lct, lt, (ft ? (fct/ft)*100 : 0), fct, ft; \
		}' target/lcov.info



