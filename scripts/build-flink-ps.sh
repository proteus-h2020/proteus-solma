#!/bin/bash


if [[ "$OSTYPE" == "linux-gnu" ]]; then 
	sed -i 's/addSbtPlugin(\"com.eed3si9n\" \% \"sbt-assembly\" \% \"0.14.3\")/addSbtPlugin(\"com.eed3si9n\" \% \"sbt-assembly\" % \"0.14.5\")/g' external/flink-parameter-server/project/assembly.sbt
	grep -q isSnapshot external/flink-parameter-server/build.sbt || sed -i 's/organization := \"hu.sztaki.ilab\"\,/organization := \"hu.sztaki.ilab\"\, isSnapshot := true,/g' external/flink-parameter-server/build.sbt
elif [[ "$OSTYPE" == "darwin"* ]]; then
	sed -i ".sbt" 's/addSbtPlugin(\"com.eed3si9n\" \% \"sbt-assembly\" \% \"0.14.3\")/addSbtPlugin(\"com.eed3si9n\" \% \"sbt-assembly\" % \"0.14.5\")/g' external/flink-parameter-server/project/assembly.sbt
	grep -q isSnapshot external/flink-parameter-server/build.sbt || sed -i ".sbt" 's/organization := \"hu.sztaki.ilab\"\,/organization := \"hu.sztaki.ilab\"\, isSnapshot := true,/g' external/flink-parameter-server/build.sbt
else
	echo "Unsupported OS for sed - please try to extend this script"
	exit 1
fi
cd external/flink-parameter-server
sbt clean publishM2
