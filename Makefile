TARGET = mymp

MPICC = mpicc
CFLAGS = -O3

SOURCES := main.c

default:
	$(MPICC) $(CFLAGS) -o $(TARGET) $(SOURCES) -fopenmp

run: 
	$ mpirun -np 5 ./$(TARGET) 

.PHONY: clean
clean:
	$ rm -f $(TARGET)
	@echo "Project Cleaned"
