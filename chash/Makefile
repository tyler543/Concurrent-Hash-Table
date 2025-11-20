CC = gcc
CFLAGS = -Wall -Wextra -pthread -g
SRCS = chash.c hash_table.c
OBJS = $(SRCS:.c=.o)
TARGET = chash

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -o $(TARGET) $(OBJS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET) hash.log

.PHONY: all clean
