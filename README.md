# laughingman7743-blog

Source for [blog.laughingman7743.org](https://blog.laughingman7743.org), built with [Zola](https://www.getzola.org/) and the [Apollo theme](https://github.com/not-matthias/apollo).
Posts live in [`content/posts/`](content/posts/).
The original publication dates are retained for the articles imported from Hatena Blog.
The Navidrome article is an English translation of the original Japanese post.

## Local development

Install Zola 0.23.2, then clone the theme and start a local server:

```sh
git submodule update --init --recursive
zola serve
```

Check a production build before opening a pull request:

```sh
zola check --skip-external-links
zola build
```

All changes go through pull requests.
Merging into `main` triggers the production deployment on Cloudflare Pages.

## Cloudflare Pages

Connect this GitHub repository to a Pages project with these build settings:

| Setting | Value |
| --- | --- |
| Production branch | `main` |
| Framework | Zola |
| Build command | `if [ "$CF_PAGES_BRANCH" = "main" ]; then zola build; else zola build --base-url "$CF_PAGES_URL"; fi` |
| Build output directory | `public` |
| Environment variable | `ZOLA_VERSION=0.23.2` |

The preview build command makes links point to the pull request's preview deployment.
Canonical URLs in the HTML still point to the production domain.

Add `blog.laughingman7743.org` to the Pages project's **Custom domains** before creating its DNS record.
In Route 53, create a CNAME record named `blog` for `laughingman7743.org` that targets the Pages project's `<project-name>.pages.dev` hostname.
The domain's nameservers and the existing `navidrome.laughingman7743.org` records do not need to change.

The [migration map](MIGRATION.md) lists the old Hatena URLs and their new paths.
