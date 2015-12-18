# Change Log


## Unreleased

### Added

* [`b00fd12`](https://github.com/nchammas/flintrock/commit/b00fd128f36e0a05dafca69b26c4d1b190fa42c9): Added `--assume-yes` option to the `launch` command. Use `--assume-yes` to tell Flintrock to automatically destroy the cluster if there are problems during launch.

### Fixed

* [#69](https://github.com/nchammas/flintrock/pull/69): Automatically retry Hadoop download from flaky Apache mirrors.
* [`0df7004`](https://github.com/nchammas/flintrock/commit/0df70043f3da215fe699165bc961bd0c4ba4ea88): Delete unneeded security group after a cluster is destroyed.


## [0.1.0](https://github.com/nchammas/flintrock/releases/tag/v0.1.0) - 2015-12-11

* Initial release.
