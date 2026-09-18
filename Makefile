CC := gcc

SRC_DIR := src
OUT_DIR := out
LOG_DIR := logs
INC_DIR := inc
BENCH_DIR := benchmark
BIN_DIR := .

CFLAGS := -std=c17 -Wall -Wextra -Wpedantic -I $(INC_DIR)

SRCS := $(wildcard $(SRC_DIR)/*.c)
OBJS := $(patsubst $(SRC_DIR)/%.c,$(OUT_DIR)/%.o,$(SRCS))
#OBJS := $(SRCS:$(SRC_DIR)/%.cpp=$(OUT_DIR)/%.o)
DEPS := $(OBJS:.o=.d)

TARGET := $(BIN_DIR)/httpserver

ifeq ($(DEBUG), 1)
	CFLAGS += -g -DDEBUG
else
	CFLAGS += -O2 -DNDEBUG
endif

# SAN=asan|tsan — сборка с санитайзерами (см. README, раздел про инструменты).
# ASan и TSan несовместимы в одной сборке — это осознанно два разных прогона.
ifeq ($(SAN), asan)
	CFLAGS += -fsanitize=address,undefined -fno-omit-frame-pointer -g -O1
	LDFLAGS += -fsanitize=address,undefined
endif
ifeq ($(SAN), tsan)
	# -no-pie: на части систем TSan падает с "unexpected memory mapping" на
	# PIE-бинарях из-за конфликта его фиксированной карты shadow-памяти со
	# случайным адресом загрузки — известное ограничение инструмента, не баг.
	CFLAGS += -fsanitize=thread -g -O1 -fno-pie
	LDFLAGS += -fsanitize=thread -no-pie
endif

all: dirs create $(TARGET)

create:
	@touch $(LOG_DIR)/server.log

dirs:
	@mkdir -p $(OUT_DIR) $(LOG_DIR)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ -pthread $(LDFLAGS)

$(OUT_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -MMD -MP -c $< -o $@

-include $(DEPS)

.PHONY: all dirs clean run

clean:
	rm -rf $(OUT_DIR)/*.o $(OUT_DIR)/*.d $(OUT_DIR)/*/*.o $(OUT_DIR)/*/*.d $(TARGET) $(OUT_DIR) $(LOG_DIR)/* $(LOG_DIR) $(BENCH_DIR)/*.png $(BENCH_DIR)/*.tex  $(BENCH_DIR)/*.pptx

run: all
	./$(TARGET)

debug: CXXFLAGS += -g -DDEBUG 
debug: CXXFLAGS := $(filter-out -O2 -DNDEBUG,$(CXXFLAGS))
debug: all

