# Change Log


## [Unreleased](https://github.com/nchammas/flintrock/compare/v0.3.0...master)

### Added

* [#98](https://github.com/nchammas/flintrock/pull/98)/[#99](https://github.com/nchammas/flintrock/pull/99): You can now specify `latest` for `--spark-git-commit` and Flintrock will automatically build Spark on your cluster at the latest commit. This feature is only available for repos hosted on GitHub.
* [#94](https://github.com/nchammas/flintrock/pull/94): Flintrock now supports launching clusters into non-default VPCs.

### Changed

* [#86](https://github.com/nchammas/flintrock/pull/86): Flintrock now correctly catches when spot requests fail and bubbles up an appropriate error message.
* [#93](https://github.com/nchammas/flintrock/pull/93)/[#97](https://github.com/nchammas/flintrock/pull/97): Fixed the ability to build Spark from git. (It was broken for recent commits.)
* [#96](https://github.com/nchammas/flintrock/pull/96): Flintrock launches now work correctly when the default Python on the cluster is Python 3.


## [0.3.0](https://github.com/nchammas/flintrock/compare/v0.2.0...v0.3.0) - 2016-02-14

### Changed

* [`eca59fc`](https://github.com/nchammas/flintrock/commit/eca59fc0052874d9aa48b7d4d7d79192b5e609d1), [`3cf6ee6`](https://github.com/nchammas/flintrock/commit/3cf6ee64162ceaac6429d79c3bc6ef25988eaa8e): Tweaked a few things so that Flintrock can launch 200+ node clusters without hitting certain limits.


## [0.2.0](https://github.com/nchammas/flintrock/compare/v0.1.0...v0.2.0) - 2016-02-07

### Added

* [`b00fd12`](https://github.com/nchammas/flintrock/commit/b00fd128f36e0a05dafca69b26c4d1b190fa42c9): Added `--assume-yes` option to the `launch` command. Use `--assume-yes` to tell Flintrock to automatically destroy the cluster if there are problems during launch.

### Changed

* [#69](https://github.com/nchammas/flintrock/pull/69): Automatically retry Hadoop download from flaky Apache mirrors.
* [`0df7004`](https://github.com/nchammas/flintrock/commit/0df70043f3da215fe699165bc961bd0c4ba4ea88): Delete unneeded security group after a cluster is destroyed.
* [`244f734`](https://github.com/nchammas/flintrock/commit/244f7345696d1b8cec1d1b575a304b9bd9a77840): Default HDFS not to install. Going forward, Spark will be the only service that Flintrock installs by default. Defaults can easily be changed via Flintrock's config file.
* [`de33412`](https://github.com/nchammas/flintrock/commit/de3341221ca8d57f5a465b13f07c8e266ae11a59): Flintrock installs services, not modules. The terminology has been updated accordingly throughout the code and docs. Update your config file to use `services` instead of `modules`. **Warning**: Flintrock will have problems managing existing clusters that were launched with versions of Flintrock from before this change.
* [#73](https://github.com/nchammas/flintrock/pull/73): Major refactoring of Flintrock internals.
* [#74](https://github.com/nchammas/flintrock/pull/74): Flintrock now catches common configuration problems upfront and provides simple error messages, instead of barfing out errors from EC2 or launching broken clusters.
* [`bf766ba`](https://github.com/nchammas/flintrock/commit/bf766ba48f12a8752c2e32f9b3daf29501c30866): Fixed a bug in how Flintrock polls SSH availability from Linux. Cluster launches now work from Linux as intended. 


## [0.1.0](https://github.com/nchammas/flintrock/releases/tag/v0.1.0) - 2015-12-11

* Initial release.
