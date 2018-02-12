#!/bin/bash

BASE_DIR=$(dirname $0)/..


echo "building Flink PS on $TRAVIS_OS_NAME at $BASE_DIR"

git submodule update --init --recursive

sed -i 's/lazy val flinkVersion = \"1.2.0\"/lazy val flinkVersion = \"1.4.0\"/g' $BASE_DIR/external/flink-parameter-server/build.sbt
sed -i 's/addSbtPlugin(\"com.eed3si9n\" \% \"sbt-assembly\" \% \"0.14.3\")/addSbtPlugin(\"com.eed3si9n\" \% \"sbt-assembly\" % \"0.14.5\")/g' $BASE_DIR/external/flink-parameter-server/project/assembly.sbt
grep -q isSnapshot $BASE_DIR/external/flink-parameter-server/build.sbt || sed -i 's/organization := \"hu.sztaki.ilab\"\,/organization := \"hu.sztaki.ilab\"\, \
isSnapshot := true,/g' $BASE_DIR/external/flink-parameter-server/build.sbt
grep -q ThisBuild $BASE_DIR/external/flink-parameter-server/build.sbt || sed -i 's/lazy val breezeVersion = \"0.13\"/lazy val breezeVersion = \"0.13\" \
resolvers in ThisBuild ++= Seq(Resolver.mavenLocal)/g' $BASE_DIR/external/flink-parameter-server/build.sbt

cd $BASE_DIR/external/flink-parameter-server
sbt clean publishM2
