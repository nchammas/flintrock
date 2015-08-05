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

Sometimes, you just wanna write some code. Just keep these guidelines in mind before you do.

### Trivial bug fixes or changes

If you're making a small change, go right ahead and open that pull request. There's no need to coordinate beforehand.

### New features, non-trivial changes

There are a few things you should do before diving in to write a new feature or implement some non-trivial change.

#### Coordinate first

Coordinating first means starting a discussion with the core developers to get a sense of how to approach the problem you want to work on.

If you don't do this and just submit a pull request out of the blue, there is a good chance you will write something that is unwanted, either because it doesn't fit the project, or because it was implemented in an undesirable way.

This doesn't mean that you need to wait for some official blessing before doing any interesting work. It just means that your chances of getting your work merged rise considerably when that work has had some input from those closest to the project.

#### Weigh the maintenance burden

Programming can be like intercourse. A neat new feature can cranked out after a passionate night of coding, but -- if accepted into the project -- it has to be maintained for years, often at much greater cumulative cost than what the initial implementation took.

When building something new, don't just consider the value it will provide. Consider also how much work it will take to keep it working over the years. Is it worth it in the long run? This is doubly important if you don't see yourself sticking around to take care of your baby. How easy will it be for others take responsibility for your work?

#### Expect many revisions

If you are adding or touching lots of code, then be prepared to go through many rounds of revisions before your pull request is accepted. This is normal, especially as you are still getting acquainted with the project's standards and style.

### Expanding the support matrix

We will generally reject contributions that expand the number of operating systems, configurations, or languages that Flintrock supports, because they impose a large maintenance burden over the project's lifespan.

In some cases, this will mean rejecting contributions that might significantly expand the potential user base for the project, for example, like adding Python 2 support.

We accept this tradeoff because we have seen popular open source projects go to decay because their maintenance burden grew large enough to kill the fun of the project for the core developers.

Small open source projects like Flintrock, which do not have the backing of company, run on the free time and interest of contributors. Keeping the project's maintenance burden as small as possible, sometimes at the cost of reach, makes it more likely that contributors will continue to find the project fun and worth contributing to.

That said, we certainly do want to support enough environments to keep the project relevant and usable to many users.
