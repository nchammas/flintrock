# Change Log

## [Unreleased]

[Unreleased]: https://github.com/nchammas/flintrock/compare/v2.0.0...master

### Changed

* [#348]: Bumped default Spark to 3.2; dropped support for Python 3.6; added CI build for Python 3.10.

[#348]: https://github.com/nchammas/flintrock/pull/348

## [2.0.0] - 2021-06-10

[2.0.0]: https://github.com/nchammas/flintrock/compare/v1.0.0...v2.0.0

### Added

* [#296]: Added support for launching clusters into private VPCs. This includes new infrastructure added in [#302] to support testing against private VPCs.
* [#307]: Added support for Hadoop/HDFS 3.x.
* [#315]: Added a new `--ec2-spot-request-duration` option to support setting the EC2 spot request duration.
* [#316]: Added a new `--java-version` option and support for Java 11.
* [#323]: Flintrock now automatically selects the correct build of Spark to use, based on the version of Hadoop/HDFS that you specify.
* [#324]: Flintrock now supports S3 URLs as a download source for Hadoop or Spark. This makes it easy to host your own copies of the Hadoop and Spark release builds in a private bucket.

[#296]: https://github.com/nchammas/flintrock/pull/296
[#302]: https://github.com/nchammas/flintrock/pull/302
[#307]: https://github.com/nchammas/flintrock/pull/307
[#315]: https://github.com/nchammas/flintrock/pull/315
[#316]: https://github.com/nchammas/flintrock/pull/316
[#323]: https://github.com/nchammas/flintrock/pull/323
[#324]: https://github.com/nchammas/flintrock/pull/324

### Changed

* [#285]: Flintrock now configures cluster nodes to use private IP addresses for internal communication. This should improve the reliability of cluster launches and restarts.
* [#304]: Fixed a bug in how `UserData` scripts are submitted to new cluster slaves.
* [#311]: Changed how Flintrock manages its own security groups to reduce the likelihood of hitting any limits on the number of rules per security group.
* [#326]: Switched some internals from using host names to IP addresses, which should improve Flintrock's behavior when running from an EC2 host.
* [#329]: Dropped support for Python 3.5 and added automated testing for Python 3.8 and 3.9.
* [#334]: Flintrock now ensures that `python3` is available on launched clusters and sets that as the default Python that PySpark will use.

[#285]: https://github.com/nchammas/flintrock/pull/285
[#304]: https://github.com/nchammas/flintrock/pull/304
[#311]: https://github.com/nchammas/flintrock/pull/311
[#326]: https://github.com/nchammas/flintrock/pull/326
[#329]: https://github.com/nchammas/flintrock/pull/329
[#334]: https://github.com/nchammas/flintrock/pull/334

## [1.0.0] - 2020-01-11

[1.0.0]: https://github.com/nchammas/flintrock/compare/v0.11.0...v1.0.0

### Changed

* [#297]: Dropped support for Python 3.4.
* [#252]: Flintrock now pins all its transitive dependencies via the files under `requirements/`. This is useful for users who want to build Flintrock themselves.

[#297]: https://github.com/nchammas/flintrock/pull/297
[#252]: https://github.com/nchammas/flintrock/pull/252

## [0.11.0] - 2018-12-02

[0.11.0]: https://github.com/nchammas/flintrock/compare/v0.10.0...v0.11.0

### Changed

* [#258], [#268]: Fixed up support for Python 3.7.
* [#264]: Fixed a logging error in `flintrock describe --master-hostname-only`.
* [#277]: Fixed a bug in resolving client IP addresses from behind proxy.

[#258]: https://github.com/nchammas/flintrock/pull/258
[#264]: https://github.com/nchammas/flintrock/pull/264
[#268]: https://github.com/nchammas/flintrock/pull/268
[#277]: https://github.com/nchammas/flintrock/pull/277

## [0.10.0] - 2018-07-15

[0.10.0]: https://github.com/nchammas/flintrock/compare/v0.9.0...v0.10.0

### Added

* [#242]: Flintrock is now available on Homebrew:
  ```
  brew install flintrock
  ```
  This is a community-supported distribution.

[#242]: https://github.com/nchammas/flintrock/pull/242

### Changed

* [#224]: Fixed a problem with some Flintrock config combinations
  related to Hadoop.
* [#232]: When you destroy a cluster, Flintrock now waits until the
  instances are completely terminated before returning.
* [#234]: Flintrock now tries more times by default to connect via
  SSH, which should provide more launch stability in certain
  environments.
* [#246]: Fixed some bugs with `flintrock describe` that are exposed
  when a cluster is transitioning states (e.g. from running to
  terminated).
* [#249]: **Flintrock now downloads both Spark and Hadoop from Apache
  mirrors by default.** This is a significant change. You can read the
  background on what prompted this change in [#238].
* [#254]: Flintrock no longer configures hadoop-aws automatically due
  to version incompatibilities that are difficult to resolve
  automatically. Instead, the README now provides additional guidance
  on using `s3a://`.
* [#259]: Flintrock now correctly ignores tiny devices that show up
  on some instance types, like the M5 series on EC2. This fixes the
  problems Flintrock had getting HDFS to work on those instance
  types.

[#224]: https://github.com/nchammas/flintrock/pull/224
[#232]: https://github.com/nchammas/flintrock/pull/232
[#234]: https://github.com/nchammas/flintrock/pull/234
[#238]: https://github.com/nchammas/flintrock/pull/238
[#246]: https://github.com/nchammas/flintrock/pull/246
[#249]: https://github.com/nchammas/flintrock/pull/249
[#254]: https://github.com/nchammas/flintrock/pull/254
[#259]: https://github.com/nchammas/flintrock/pull/259

## [0.9.0] - 2017-08-06

[0.9.0]: https://github.com/nchammas/flintrock/compare/v0.8.0...v0.9.0

### Added

* [#178]: You can now see additional output during launch and other
  operations with the new `--debug` option.
* [#185]: Added a new mount point under `/media/tmp` that can be used
  when `/tmp` is not big enough.
* [#186]: You can now tag your clusters with arbitrary tags on launch
  using the new `--ec2-tag` option. (Remember: As with all options,
  you can also set this via `flintrock configure`.)
* [#191]: You can now specify the size of the root EBS volume with the
  new `--ec2-min-root-ebs-size-gb` option.
* [#181]: You can now set the number of executors per worker with
  `--spark-executor-instances`.

[#178]: https://github.com/nchammas/flintrock/pull/178
[#185]: https://github.com/nchammas/flintrock/pull/185
[#186]: https://github.com/nchammas/flintrock/pull/186
[#191]: https://github.com/nchammas/flintrock/pull/191
[#181]: https://github.com/nchammas/flintrock/pull/181

### Changed

* [#195]: After launching a new cluster, Flintrock now shows the
  master address and login command.
* [#196], [#197]: Fixed some bugs that were preventing Flintrock from
  launching Spark clusters at a specific commit.
* [#204]: Flintrock now automatically retries starting the Spark and
  HDFS masters if it encounters common issues with bringing the
  cluster up. This greatly improves launch and restart reliability.
* [#208]: Flintrock now provides a hint with possible causes for
  certain SSH errors.

[#195]: https://github.com/nchammas/flintrock/pull/195
[#196]: https://github.com/nchammas/flintrock/pull/196
[#197]: https://github.com/nchammas/flintrock/pull/197
[#204]: https://github.com/nchammas/flintrock/pull/204
[#208]: https://github.com/nchammas/flintrock/pull/208

## [0.8.0] - 2017-02-11

[0.8.0]: https://github.com/nchammas/flintrock/compare/v0.7.0...v0.8.0

### Added

* [#180]: Accessing data on S3 from your Flintrock cluster is now much
  easier! Just configure Flintrock to use Hadoop 2.7+ (which is the
  default) and an appropriate IAM role, and you'll be able to access
  paths on S3 using the new `s3a://` prefix. [Check the README] for
  more information.
* [#176], [#187]: Flintrock now supports users with non-standard home
  directories.

[#180]: https://github.com/nchammas/flintrock/pull/180
[#176]: https://github.com/nchammas/flintrock/pull/176
[#187]: https://github.com/nchammas/flintrock/pull/187
[Check the README]: https://github.com/nchammas/flintrock/tree/v0.8.0#accessing-data-on-s3

### Changed

* [#168]: Flintrock now does a better job of cleaning up after
  interrupted operations.
* [#179], [#184]: Flintrock can now clean up malformed Flintrock
  clusters.
* [`6b426ae`]: We fixed an issue affecting some users of Flintrock's
  standalone package that caused Flintrock to intermittently throw
  `ImportError`s.

[#168]: https://github.com/nchammas/flintrock/pull/168
[#179]: https://github.com/nchammas/flintrock/pull/179
[#184]: https://github.com/nchammas/flintrock/pull/184
[`6b426ae`]: https://github.com/nchammas/flintrock/commit/6b426aedc7e92b434021cc09c6e7eb181fca7eef

## [0.7.0] - 2016-11-15

[0.7.0]: https://github.com/nchammas/flintrock/compare/v0.6.0...v0.7.0

### Added

* [#146]: Flintrock now ensures that launched clusters have Java 8 or
  higher installed.
* [#149]: You can now specify an [EC2 user data] script to use on launch
  with the new `--ec2-user-data` option.

[#146]: https://github.com/nchammas/flintrock/pull/146
[#149]: https://github.com/nchammas/flintrock/pull/149
[EC2 user data]: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/user-data.html

### Changed

* [#154], [#155], [#156]: Flintrock now provides friendly error messages
  when it encounters common configuration or setup problems.

[#154]: https://github.com/nchammas/flintrock/pull/154
[#155]: https://github.com/nchammas/flintrock/pull/155
[#156]: https://github.com/nchammas/flintrock/pull/156

## [0.6.0] - 2016-08-28

[0.6.0]: https://github.com/nchammas/flintrock/compare/v0.5.0...v0.6.0

### Added

* [#115]: Flintrock can now resize existing clusters with the new
  `add-slaves` and `remove-slaves` commands.

[#115]: https://github.com/nchammas/flintrock/pull/115

### Changed

* [#115]: If you lost your master somehow, Flintrock can now still
  destroy the cluster.
* [#115]: You can no longer launch clusters with 0 slaves. The
  implementation was broken. We may fix and add this capability back
  in the future.

## [0.5.0] - 2016-07-20

[0.5.0]: https://github.com/nchammas/flintrock/compare/v0.4.0...v0.5.0

### Added

* [#118]: You can now specify `--hdfs-download-source` (or the
  equivalent in your config file) to tell Flintrock to download Hadoop
  from a specific URL when launching your cluster.
* [#125]: You can now specify `--spark-download-source` (or the
  equivalent in your config file) to tell Flintrock to download Spark
  from a specific URL when launching your cluster.
* [#112]: You can now specify `--ec2-security-group` to associate
  additional security groups with your cluster on launch.

[#118]: https://github.com/nchammas/flintrock/pull/118
[#125]: https://github.com/nchammas/flintrock/pull/125
[#112]: https://github.com/nchammas/flintrock/pull/112

### Changed

* [#103], [#114]: Flintrock now opens port 6066 and 7077 so local
  clients like Apache Zeppelin can connect directly to the Spark
  master on the cluster.
* [#122]: Flintrock now automatically adds executables like
  `spark-submit`, `pyspark`, and `hdfs` to the default `PATH`, so
  they're available to call right when you login to the cluster.

[#103]: https://github.com/nchammas/flintrock/pull/103
[#114]: https://github.com/nchammas/flintrock/pull/114
[#122]: https://github.com/nchammas/flintrock/pull/122

## [0.4.0] - 2016-03-27

[0.4.0]: https://github.com/nchammas/flintrock/compare/v0.3.0...v0.4.0

### Added

* [#98], [#99]: You can now specify `latest` for `--spark-git-commit`
  and Flintrock will automatically build Spark on your cluster at the
  latest commit. This feature is only available for Spark repos
  hosted on GitHub.
* [#94]: Flintrock now supports launching clusters into non-default
  VPCs.

[#94]: https://github.com/nchammas/flintrock/pull/94
[#98]: https://github.com/nchammas/flintrock/pull/98
[#99]: https://github.com/nchammas/flintrock/pull/99

### Changed

* [#86]: Flintrock now correctly catches when spot requests fail and
  bubbles up an appropriate error message.
* [#93], [#97]: Fixed the ability to build Spark from git. (It was
  broken for recent commits.)
* [#96], [#100]: Flintrock launches should now work correctly whether
  the default Python on the cluster is Python 2.7 or Python 3.4+.

[#86]: https://github.com/nchammas/flintrock/pull/86
[#93]: https://github.com/nchammas/flintrock/pull/93
[#96]: https://github.com/nchammas/flintrock/pull/96
[#97]: https://github.com/nchammas/flintrock/pull/97
[#100]: https://github.com/nchammas/flintrock/pull/100

## [0.3.0] - 2016-02-14

[0.3.0]: https://github.com/nchammas/flintrock/compare/v0.2.0...v0.3.0

### Changed

* [`eca59fc`], [`3cf6ee6`]: Tweaked a few things so that Flintrock
  can launch 200+ node clusters without hitting certain limits.

[`eca59fc`]: https://github.com/nchammas/flintrock/commit/eca59fc0052874d9aa48b7d4d7d79192b5e609d1
[`3cf6ee6`]: https://github.com/nchammas/flintrock/commit/3cf6ee64162ceaac6429d79c3bc6ef25988eaa8e

## [0.2.0] - 2016-02-07

[0.2.0]: https://github.com/nchammas/flintrock/compare/v0.1.0...v0.2.0

### Added

* [`b00fd12`]: Added `--assume-yes` option to the `launch` command.
  Use `--assume-yes` to tell Flintrock to automatically destroy the
  cluster if there are problems during launch.

[`b00fd12`]: https://github.com/nchammas/flintrock/commit/b00fd128f36e0a05dafca69b26c4d1b190fa42c9

### Changed

* [#69]: Automatically retry Hadoop download from flaky Apache
  mirrors.
* [`0df7004`]: Delete unneeded security group after a cluster is
  destroyed.
* [`244f734`]: Default HDFS not to install. Going forward, Spark will
  be the only service that Flintrock installs by default. Defaults can
  easily be changed via Flintrock's config file.
* [`de33412`]: Flintrock installs services, not modules. The
  terminology has been updated accordingly throughout the code and
  docs. Update your config file to use `services` instead of
  `modules`. **Warning**: Flintrock will have problems managing
  existing clusters that were launched with versions of Flintrock from
  before this change.
* [#73]: Major refactoring of Flintrock internals.
* [#74]: Flintrock now catches common configuration problems upfront
  and provides simple error messages, instead of barfing out errors
  from EC2 or launching broken clusters.
* [`bf766ba`]: Fixed a bug in how Flintrock polls SSH availability
  from Linux. Cluster launches now work from Linux as intended.

[#69]: https://github.com/nchammas/flintrock/pull/69
[`0df7004`]: https://github.com/nchammas/flintrock/commit/0df70043f3da215fe699165bc961bd0c4ba4ea88
[`244f734`]: https://github.com/nchammas/flintrock/commit/244f7345696d1b8cec1d1b575a304b9bd9a77840
[`de33412`]: https://github.com/nchammas/flintrock/commit/de3341221ca8d57f5a465b13f07c8e266ae11a59
[#73]: https://github.com/nchammas/flintrock/pull/73
[#74]: https://github.com/nchammas/flintrock/pull/74
[`bf766ba`]: https://github.com/nchammas/flintrock/commit/bf766ba48f12a8752c2e32f9b3daf29501c30866

## [0.1.0] - 2015-12-11

[0.1.0]: https://github.com/nchammas/flintrock/releases/tag/v0.1.0

* Initial release.
