#!/bin/sh

#
# basic map-reduce test
#
# purpose:
# 1.正常测试wc.so配合mrsequential和mrmaster,mrworker结果是否一致，即单词出现个数统计
# mr-x-x => {"Key":"state","Value":"1"}
# mr-out-x => state 119
# mr-wc-all => state 119
#
# 2.正常测试indexer.so配合mrsequential和mrmaster,mrworker结果是否一致，即单词所在文本统计
# mr-x-x => {"Key":"ask","Value":"../pg-being_ernest.txt"}
# mr-out-x => Constantinople 2 ../pg-frankenstein.txt,../pg-tom_sawyer.txt
# mr-indexer-all => yonder 4 ../pg-grimm.txt,../pg-huckleberry_finn.txt,../pg-sherlock_holmes.txt,../pg-tom_sawyer.txt
#
# 3.检测map任务是否并行
# mr-x-x => {"Key":"times-13529","Value":"1618476163.4"}
# mr-x-x => {"Key":"parallel-13529","Value":"2"}
# mr-out-x => times-13529 1618476161.4 1618476162.4 1618476163.4 1618476164.4
# mr-out-x => parallel-13529 2 2 2 2
#
# 4.检测reduce任务是否并行
# mr-x-x => {"Key":"c","Value":"1"}
# mr-out-x => c 2
#
# 5.目的是检测MapReduce分布式系统的恢复能力

RACE=

# uncomment this to run the tests with the Go race detector.
#RACE=-race

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build $RACE mrmaster.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

# first word-count

# generate the correct output
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

# timeout命令运行180秒，如果命令没有结束，将在2秒后终止命令
timeout -k 2s 180s ../mrmaster ../pg*txt &

# give the master time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &

# wait for one of the processes to exit.
# under bash, this waits for all processes,
# including the master.
wait

# the master or a worker has exited. since workers are required
# to exit when a job is completely finished, and not before,
# that means the job has finished.

sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and master to exit.
wait ; wait ; wait

# now indexer
rm -f mr-*

# generate the correct output
../mrsequential ../../mrapps/indexer.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

timeout -k 2s 180s ../mrmaster ../pg*txt &
sleep 1

# start multiple workers
timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so &
timeout -k 2s 180s ../mrworker ../../mrapps/indexer.so

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait ; wait


echo '***' Starting map parallelism test.

rm -f mr-out* mr-worker*

timeout -k 2s 180s ../mrmaster ../pg*txt &
sleep 1

timeout -k 2s 180s ../mrworker ../../mrapps/mtiming.so &
timeout -k 2s 180s ../mrworker ../../mrapps/mtiming.so

NT=`cat mr-out* | grep '^times-' | wc -l | sed 's/ //g'`
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait ; wait


echo '***' Starting reduce parallelism test.

rm -f mr-out* mr-worker*

timeout -k 2s 180s ../mrmaster ../pg*txt &
sleep 1

timeout -k 2s 180s ../mrworker ../../mrapps/rtiming.so &
timeout -k 2s 180s ../mrworker ../../mrapps/rtiming.so

NT=`cat mr-out* | grep '^[a-z] 2' | wc -l | sed 's/ //g'`
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait ; wait


# generate the correct output
../mrsequential ../../mrapps/nocrash.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

echo '***' Starting crash test.

rm -f mr-done
(timeout -k 2s 180s ../mrmaster ../pg*txt ; touch mr-done ) &
sleep 1

# start multiple workers
timeout -k 2s 180s ../mrworker ../../mrapps/crash.so &

# mimic rpc.go's masterSock()
SOCKNAME=/var/tmp/824-mr-`id -u`

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

( while [ -e $SOCKNAME -a ! -f mr-done ]
  do
    timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
    sleep 1
  done ) &

while [ -e $SOCKNAME -a ! -f mr-done ]
do
  timeout -k 2s 180s ../mrworker ../../mrapps/crash.so
  sleep 1
done

wait
wait
wait

rm $SOCKNAME
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
