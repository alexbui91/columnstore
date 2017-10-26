################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
CPP_SRCS += \
../Column.cpp \
../ColumnBase.cpp \
../Dictionary.cpp \
../Table.cpp \
../main.cpp 

C_SRCS += \
../PackedArray.c 

OBJS += \
./Column.o \
./ColumnBase.o \
./Dictionary.o \
./PackedArray.o \
./Table.o \
./main.o 

CPP_DEPS += \
./Column.d \
./ColumnBase.d \
./Dictionary.d \
./Table.d \
./main.d 

C_DEPS += \
./PackedArray.d 


# Each subdirectory must supply rules for building sources it contributes
%.o: ../%.cpp
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C++ Compiler'
	g++ -O0 -g3 -Wall -c -fmessage-length=0 -std=c++11 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '

%.o: ../%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


