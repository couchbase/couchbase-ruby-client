We've decided to use "gerrit" for our code review system, making it
easier for all of us to contribute with code and comments.

  1. Visit http://review.couchbase.org and "Register" for an account
  2. Review http://review.couchbase.org/static/individual_agreement.html
  3. Agree to agreement by visiting http://review.couchbase.org/#/settings/agreements
  4. If you do not receive an email, please contact us
  5. Check out the `couchbase-ruby-client` area http://review.couchbase.org/#/q/status:open+project:couchbase-ruby-client,n,z
  6. Join us on IRC at #libcouchbase on Freenode :-)

We normally don't go looking for stuff in gerrit, so you should add at
least me `"Sergey Avseyev" <sergey.avseyev@gmail.com>` as a reviewer
for your patch (and I'll know who else to add and add them for you).

## Contributing Using Repo Tool

Follow ["Uploading Changes" guide][1] on the site if you have some code to contribute.

All you should need to set up your development environment should be:

    ~ % mkdir couchbase-ruby
    ~ % cd couchbase-ruby
    ~/couchbase-ruby % repo init -u git://github.com/trondn/manifests.git -m ruby.xml
    ~/couchbase-ruby % repo sync
    ~/couchbase-ruby % repo start my-branch-name --all
    ~/couchbase-ruby % make

This will build the latest version of `libcouchbase`,
`couchbase-ruby-client` and `couchbase-ruby-client` libraries. You must
have a C and C++ compiler installed, automake, autoconf.

If you have to make any changes just commit them before you upload
them to gerrit with the following command:

    ~/couchbase-ruby/client % repo upload

You might experience a problem trying to upload the patches if you've
selected a different login name at http://review.couchbase.org than
your login name. Don't worry, all you need to do is to add the
following to your ~/.gitconfig file:

    [review "review.couchbase.org"]
        username = YOURNAME

## Contributing Using Plain Git

If you not so familiar with repo tool and its workflow there is
alternative way to do the same job. Lets assume you have installed
couchbase gem and libcouchbase from official packages and would you to
contribute to couchbase-client gem only. Then you just need to complete
gerrit registration steps above and clone the source repository
(remember the repository on github.com is just a mirror):

    ~ % git clone ssh://YOURNAME@review.couchbase.org:29418/couchbase-ruby-client.git

Install [`commit-msg` hook][2]:

    ~/couchbase-ruby-client % scp -p -P 29418 YOURNAME@review.example.com:hooks/commit-msg .git/hooks/

Make your changes and upload them for review:

    ~/couchbase-ruby-client % git commit
    ~/couchbase-ruby-client % git push origin HEAD:refs/for/master

If you need to fix or add something to your patch, do it and re-upload
the changes (all you need is to keep `Change-Id:` line the same to
allow gerrit to track the patch.

    ~/couchbase-ruby-client % git commit --amend
    ~/couchbase-ruby-client % git push origin HEAD:refs/for/master

Happy hacking!

[1]: http://review.couchbase.org/Documentation/user-upload.html
[2]: http://review.couchbase.org/Documentation/user-changeid.html
