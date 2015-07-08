# Contributing Guide

**Flintrock is still under heavy development.**

**Until we make a 0.1 release, most PRs will be rejected.** We are likely already fixing or making that thing you want to open a PR for, and major components are rapidly evolving so your PR is likely to be invalidated by upcoming changes you may not know about.

This guide was heavily inspired by the excellent [Phabricator contributor docs](https://secure.phabricator.com/book/phabcontrib/).


## Contributing Thanks

* We humans can be quite simple. A thank you motivates us and gives us energy.


## Contributing Money

[Amazon Allowance](http://www.amazon.com/b?ie=UTF8&node=11453461011)


## Contributing Bug Reports

* Provide a [short, self contained, and correct example](http://sscce.org/) of the problem you are seeing.
* Reports will otherwise likely be ignored unless they are really easy to find and fix.
* Confirm or deny an existing bug report.


## Contributing Feature Requests

### Describe your problem first, not just your solution

What are you trying to do? Explain the root problem clearly. **This is more important than describing your proposed solution.**

  When we understand your feature request in the context of what you are really trying to do, we can better evaluate any proposed solutions and perhaps even come up with a better solution that you might not see.

  Describing your original problem or use case will also help us avoid the [X-Y Problem](http://mywiki.wooledge.org/XyProblem), which can waste a lot of everyone's time.

* Chime in on an existing feature request.


## Contributing Code

Sometimes, you just wanna write some code.

### Trivial bug fixes

Go right ahead and open that pull request.

### New features, non-trivial changes

* Coordinate first, otherwise there is a good chance you will write something that is unwanted, either because it doesn't fit the project, or because it was implemented in an undesirable way.
* Weigh the maintenance burden _very carefully_. A neat new feature can cranked out after a few nights of passionate coding, but then it has to be maintained for years after that.
* Be prepared for many rounds of revisions before your pull request is accepted. This is normal.

### No

* No gratuitous customization.
* No new config options.
* No support for older versions of stuff.
