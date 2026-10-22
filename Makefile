# QNX 6 / Linux 共通の簡易 Makefile（C++03）
#   make            : ビルド
#   make clean      : 削除
# QNX 6 で socket 関数のリンクエラーが出る場合: make LDLIBS="-lsocket"
CXX      ?= g++
CXXFLAGS ?= -std=c++98 -Wall -Wextra -pedantic -O2
LDFLAGS  += -pthread
LDLIBS   ?=

TARGET = server
OBJS   = main.o qnx_multi_tcp_server.o

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJS) $(LDFLAGS) $(LDLIBS)

%.o: %.cpp qnx_multi_tcp_server.h
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(TARGET) $(OBJS)

.PHONY: clean
