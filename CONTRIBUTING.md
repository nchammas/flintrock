# Contributing Guide

There are many ways to contribute to Flintrock.

## Contributing Thanks

When we put our time and enthusiasm into an open source project like this, we hope that somewhere out there we are putting a smile on someone's face.

Most of the time we'll never know, though. When people reach out within an open source community, it's typically to report a problem, ask for help, or share an idea.

That's a bummer, because hearing first-hand that we made a positive impact on someone else's day, even if it's minor, can be a huge boost of joy and motivation.

Don't underestimate the power of a thank you. If Flintrock helped you in some way, share your story, even if it's "trivial", and know that at times this can be the most valuable way to contribute to the project.


## Contributing Money

Most projects have various kinds of tests to make sure things are working correctly. The most valuable test for an orchestration tool like Flintrock is a full acceptance test, since the fundamental thing Flintrock does is manage remote resources.

This means that as Flintrock developers we are always launching and destroying instances on some cloud provider, which costs money. Any money you contribute will go towards paying those bills.

We're still figuring out how best to accept donations for these purposes, but [Amazon Allowance](http://www.amazon.com/b?ie=UTF8&node=11453461011) looks promising.


## Contributing Bug Reports

When reporting a bug, do your best to provide a [short, self contained, and correct example](http://sscce.org/) of the problem you are seeing. Bug reports will otherwise likely be ignored, unless they are really easy to reproduce.

In addition to reporting bugs, you can also confirm or deny existing bug reports. This helps us prioritize bug fixes and understand if certain bugs are limited to certain configurations.


## Contributing Feature Requests

### Describe your problem first, not just your solution

What are you trying to do? Explain the root problem clearly. **This is more important than describing your proposed solution.**

When we understand your feature request in the context of what you are really trying to do, we can better evaluate any proposed solutions and perhaps even come up with a better solution that you might not see.

Describing your original problem or use case will also help us avoid the [X-Y Problem](http://mywiki.wooledge.org/XyProblem), which can waste a lot of everyone's time.

If you see an existing feature request that you are interested in, chime in. Your input will help us flesh out the request and understand how much demand there is for it.


## Contributing Code

Sometimes, you just wanna write some code. Just keep these guidelines in mind before you do that if you want your code contribution accepted.

### License

Unless you explicitly tell us otherwise, when you contribute code you affirm that the contribution is your original work and that you license it to the project under the project's [license](LICENSE).

Please make sure that you are OK with our license's terms before contributing code.

### Setup

If you agree to our license, the next thing you'll want to do is get Flintrock's source code and install its development dependencies.

```sh
git clone https://github.com/nchammas/flintrock
cd flintrock

python3 -m venv venv
source venv/bin/activate

pip3 install -r requirements/developer.pip
```

When you `git pull` the latest changes, don't forget to also rerun the `pip install` step so that Flintrock's dependencies stay up-to-date.

### Trivial bug fixes or changes

If you're making a small change, go right ahead and open that pull request. There's no need to coordinate beforehand.

### New features, non-trivial changes

There are a few things you should do before diving in to write a new feature or implement some non-trivial change.

### Changing dependencies

If you are changing anything about Flintrock's dependencies, be sure to update the compiled requirements using [pip-tools]:

```
pip-compile requirements/user.in -o requirements/user.pip
pip-compile requirements/developer.in -o requirements/developer.pip
pip-compile requirements/maintainer.in -o requirements/maintainer.pip
```

After doing that, there are a couple of things you'll need to do:
1. Update the compiled requirements to remove the absolute `-e file:///` paths that `pip-tools` adds and replace them with `-e .`.
2. Optionally, run `pip-sync` to make sure your environment matches what's in the compiled requirements.

[pip-tools]: https://github.com/jazzband/pip-tools

#### Coordinate first

Coordinating first means starting a discussion with the core developers to get a sense of how to approach the problem you want to work on.

If you don't do this and just submit a pull request out of the blue, there is a good chance you will write something that is unwanted, either because it doesn't fit the project, or because it was implemented in an undesirable way.

This doesn't mean that you need to wait for some official blessing before doing any interesting work. It just means that your chances of getting your work merged rise considerably when that work has had some input from those closest to the project.

#### Weigh the maintenance burden

Programming can be like intercourse. A neat new feature can cranked out after a passionate night of coding, but -- if accepted into the project -- it has to be maintained for years, often at much greater cumulative cost than what the initial implementation took.

When building something new, don't just consider the value it will provide. Consider also how much work it will take to keep it working over the years. Is it worth it in the long run? This is doubly important if you don't see yourself sticking around to take care of your baby. How easy will it be for others take responsibility for your work?

#### Capture one idea in one pull request

*Note: This section is largely a summary of the [guidance given here](https://secure.phabricator.com/book/phabflavor/article/recommendations_on_revision_control/) by Evan Priestley of the Phabricator project.*

Make sure each pull request you submit captures a single coherent idea. This limits the scope of any given pull request and makes it much easier for a reviewer to understand what you are doing and give precise feedback. Don't mix logically independent changes in the same request if they can be submitted separately.

#### Expect many revisions

If you are adding or touching lots of code, then be prepared to go through many rounds of revisions before your pull request is accepted. This is normal, especially as you are still getting acquainted with the project's standards and style.

### Test your changes

Whether your changes are big or small, you'll want to test them. Flintrock includes [tests](./tests/) which you should use.

### Don't expand the support matrix

We will generally reject contributions that expand the number of operating systems, configurations, or languages that Flintrock supports, because they impose a large maintenance burden on the project over its lifespan. In some cases this might mean rejecting contributions that could significantly expand the project's potential user base.

We accept this tradeoff because we have seen popular open source projects go to decay because their maintenance burden grew large enough to kill the fun of the project for the core developers.

Small open source projects like Flintrock, which do not have the backing of a company, run on the free time and interest of contributors. Keeping the project's maintenance burden as small as possible, sometimes at the cost of reach, makes it more likely that contributors will continue to take interest in the project for a long time. This better serves our user base over the long run.
