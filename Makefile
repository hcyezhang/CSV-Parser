CXX=g++
CPPFLAGS=-std=c++1y

all:
	$(CXX) $(CPPFLAGS) main.cc -o main

clean:
	rm main
