#!/bin/bash

if [ -z $GOPATH ]; then
    echo "FAIL: GOPATH environment variable is not set"
    exit 1
fi

if [ -n "$(go version | grep 'darwin/amd64')" ]; then
    GOOS="darwin_amd64"
elif [ -n "$(go version | grep 'linux/amd64')" ]; then
    GOOS="linux_amd64"
else
    echo "FAIL: only 64-bit Mac OS X and Linux operating systems are supported"
    exit 1
fi

# Build the lrunner binary to use to test the student's libstore implementation.
# Exit immediately if there was a compile-time error.
go install github.com/cmu440/tribbler/runners/lrunner
if [ $? -ne 0 ]; then
   echo "FAIL: code does not compile"
   exit $?
fi

# Build the srunner binary to use to test the student's storageserver implementation.
# Exit immediately if there was a compile-time error.
go install github.com/cmu440/tribbler/runners/srunner
if [ $? -ne 0 ]; then
   echo "FAIL: code does not compile"
   exit $?
fi

# Pick random port between [10000, 20000).
STORAGE_PORT=$(((RANDOM % 10000) + 10000))
STORAGE_SERVER=$GOPATH/bin/srunner
LRUNNER=$GOPATH/bin/lrunner

function startStorageServers {
    N=${#STORAGE_ID[@]}
    # Start master storage server.
    ${STORAGE_SERVER} -N=${N} -vids=${STORAGE_ID[0]} -port=${STORAGE_PORT} 2> /dev/null &
    STORAGE_SERVER_PID[0]=$!
    # Start slave storage servers.
    if [ "$N" -gt 1 ]
    then
        for i in `seq 1 $((N-1))`
        do
	    STORAGE_SLAVE_PORT=$(((RANDOM % 10000) + 10000))
            ${STORAGE_SERVER} -vids=${STORAGE_ID[$i]} -port=${STORAGE_SLAVE_PORT} -master="localhost:${STORAGE_PORT}" 2> /dev/null &
            STORAGE_SERVER_PID[$i]=$!
        done
    fi
    sleep 10
}

function stopStorageServers {
    N=${#STORAGE_ID[@]}
    for i in `seq 0 $((N-1))`
    do
        kill -9 ${STORAGE_SERVER_PID[$i]}
        wait ${STORAGE_SERVER_PID[$i]} 2> /dev/null
    done
}

# Testing delayed start.
function testDelayedStart {
    echo "Running testDelayedStart:"

    # Start master storage server.
    ${STORAGE_SERVER} -N=2 -port=${STORAGE_PORT} 2> /dev/null &
    STORAGE_SERVER_PID1=$!
    sleep 5

    # Run lrunner.
    ${LRUNNER} -port=${STORAGE_PORT} p "key:" value &> /dev/null &
    sleep 3

    # Start second storage server.
    STORAGE_SLAVE_PORT=$(((RANDOM % 10000) + 10000))
    ${STORAGE_SERVER} -master="localhost:${STORAGE_PORT}" -port=${STORAGE_SLAVE_PORT} 2> /dev/null &
    STORAGE_SERVER_PID2=$!
    sleep 5

    # Run lrunner.
    PASS=`${LRUNNER} -port=${STORAGE_PORT} g "key:" | grep value | wc -l`
    if [ "$PASS" -eq 1 ]
    then
        echo "PASS"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        echo "FAIL"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

    # Kill storage servers.
    kill -9 ${STORAGE_SERVER_PID1}
    kill -9 ${STORAGE_SERVER_PID2}
    wait ${STORAGE_SERVER_PID1} 2> /dev/null
    wait ${STORAGE_SERVER_PID2} 2> /dev/null
}

function testUtil {
    for KEY in "${KEYS[@]}"
    do
        ${LRUNNER} -port=${STORAGE_PORT} p ${KEY} value > /dev/null
        PASS=`${LRUNNER} -port=${STORAGE_PORT} g ${KEY} | grep value | wc -l`
        if [ "$PASS" -ne 1 ]
        then
            break
        fi
    done
    if [ "$PASS" -eq 1 ]
    then
        echo "PASS"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        echo "FAIL"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

}

function testRouting {
    startStorageServers
    testUtil
    stopStorageServers
}

function testRoutingWithShutdown {
    startStorageServers
    for ELEM in "${NUM_SHUTDOWN[@]}"
    do
        kill -SIGUSR1 ${STORAGE_SERVER_PID[${ELEM}]}
    done   
    sleep 10
    testUtil
    stopStorageServers
}

# Testing routing general.
function testRoutingGeneral {
    echo "Running testRoutingGeneral:"
    STORAGE_ID=('3000000000' '4000000000' '2000000000')
    KEYS=('bubble:' 'insertion:' 'merge:' 'heap:' 'quick:' 'radix:')
    testRouting
}

# Shutdown one server
function testRoutingShutdownOneServer {
    echo "Running testRoutingShutdownOneServer:"
    STORAGE_ID=('3000000000' '4000000000' '2000000000')
    KEYS=('bubble:' 'insertion:' 'merge:' 'heap:' 'quick:' 'radix:')
    NUM_SHUTDOWN=('1')
    testRoutingWithShutdown
}

# Shutdown alternate servers
function testRoutingShutdownMultiServerAlternate {
    echo "Running testRoutingShutdownMultiServerAlternate:"
    STORAGE_ID=('3500000000' '3000000000' '4500000000' '4000000000' '2000000000' '1000000000')
    KEYS=('bubble:' 'insertion:' 'merge:' 'heap:' 'quick:' 'radix:')
    NUM_SHUTDOWN=('1' '3' '5')
    testRoutingWithShutdown
}

# Shutdown multiple consecutive servers
function testRoutingShutdownMultiServerSequential {
    echo "Running testRoutingShutdownMultiServerSequential:"
    STORAGE_ID=('5000000000' '3000000000' '4500000000' '4000000000' '2000000000' '1000000000')
    KEYS=('bubble:' 'insertion:' 'merge:' 'heap:' 'quick:' 'radix:')
    NUM_SHUTDOWN=('1' '4' '5')
    testRoutingWithShutdown
}

# Shutdown all but one server. Two versions to ensure
# that the solution is crawling the ring and wrapping
# around when needed; passing both tests will ensure
# both functionalities.

# Shutdown all but one server; leave first server alive
function testRoutingShutdownAllButOneBeg {
    echo "Running testRoutingShutdownAllButOneBeg:"
    STORAGE_ID=('5000000000' '3000000000' '4500000000' '4000000000' '2000000000' '1000000000')
    KEYS=('bubble:' 'insertion:' 'merge:' 'heap:' 'quick:' 'radix:')
    NUM_SHUTDOWN=('1' '2' '3' '4' '5')
    testRoutingWithShutdown
}

# Shutdown all but one server; leave last server alive
function testRoutingShutdownAllButOneEnd {
    echo "Running testRoutingShutdownAllButOneEnd:"
    STORAGE_ID=('1000000000' '5000000000' '3000000000' '4500000000' '4000000000' '2000000000')
    KEYS=('bubble:' 'insertion:' 'merge:' 'heap:' 'quick:' 'radix:')
    NUM_SHUTDOWN=('1' '2' '3' '4' '5')
    testRoutingWithShutdown
}

# Testing routing wraparound.
function testRoutingWraparound {
    echo "Running testRoutingWraparound:"
    STORAGE_ID=('2000000000' '2500000000' '3000000000')
    KEYS=('bubble:' 'insertion:' 'merge:' 'heap:' 'quick:' 'radix:')
    testRouting
}

# Testing routing equal.
function testRoutingEqual {
    echo "Running testRoutingEqual:"
    STORAGE_ID=('3835649095' '1581790440' '2373009399' '3448274451' '1666346102' '2548238361')
    KEYS=('bubble:' 'insertion:' 'merge:' 'heap:' 'quick:' 'radix:')
    testRouting
}

function testVirtualTwoServers {
  echo "Running testVirtualTwoServers:"
  STORAGE_ID=('500000000,1500000000'  '1000000000,2000000000')
  KEYS=('yuvraj:' 'agarwal:' 'daniel:' 'berger:' 'tushar:' 'chelsea:' 'chen:'
  'karan:' 'dhabalia:' 'guoyao:' 'freddie:' 'feng:' 'zeleena:' 'kerney:'
  'dohyun:' 'kim:' 'sam:' 'amadou:' 'latyr:' 'ngom:' 'tian:' 'zhao:' 'priyatham:' 'bollimpalli:')
  testRouting
}

function testVirtualMultiServers {
  echo "Running testVirtualMultiServers:"
  STORAGE_ID=('100000000,1100000000,1600000000'  '300000000,1400000000' '500000000' '800000000,1800000000')
  KEYS=('yuvraj:' 'agarwal:' 'daniel:' 'berger:' 'tushar:' 'chelsea:' 'chen:'
  'karan:' 'dhabalia:' 'guoyao:' 'freddie:' 'feng:' 'zeleena:' 'kerney:'
  'dohyun:' 'kim:' 'sam:' 'amadou:' 'latyr:' 'ngom:' 'tian:' 'zhao:' 'priyatham:' 'bollimpalli:')
  testRouting
}

# Run tests
PASS_COUNT=0
FAIL_COUNT=0
testDelayedStart
testRoutingGeneral
testRoutingWraparound
testRoutingEqual
testVirtualTwoServers
testVirtualMultiServers
testRoutingShutdownOneServer
testRoutingShutdownMultiServerAlternate
testRoutingShutdownMultiServerSequential
testRoutingShutdownAllButOneBeg
testRoutingShutdownAllButOneEnd

echo "Passed (${PASS_COUNT}/$((PASS_COUNT + FAIL_COUNT))) tests"
