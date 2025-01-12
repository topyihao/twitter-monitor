# Twitter Monitor

Monitor the `following`, `tweet`, ~~`like`~~ and `profile` of a Twitter user and send the changes to the telegram channel.

Data is crawled from Twitter web’s GraphQL API.

(Due to the unstable return results of Twitter Web's `following` API, accounts with more than 100 followings are not recommended to use `following` monitor.)

## Deployed channel sample

https://t.me/twitter_monitor_menu

## Usage

### Setup

(Requires **python >= 3.10**)

Clone code and install dependent pip packages

```bash
git clone https://github.com/ionic-bond/twitter-monitor.git
cd twitter-monitor
pip3 install -r ./requirements.txt
```

### Prepare required tokens

- Create a Telegram bot and get it's token:

  https://t.me/BotFather

- Unofficial Twitter account auth:

  You need to prepare one or more normal twitter accounts, and then use the following command to generate auth cookies

  ```bash
  python3 main.py generate-auth-cookie --username "{username}" --password "{password}"
  ```

- If you failed to login using the above command, you can use the following method to manually export twitter cookies and import it into the project

  1. Export cookies from your browser using the plugin [Export cookie JSON file for Puppeteer](https://chromewebstore.google.com/detail/export-cookie-json-file-f/nmckokihipjgplolmcmjakknndddifde?hl=en). You will get a JSON file _x.com.cookies.json_
  2. Convert the cookies using the following command, the username and email are your twitter account username and email

  ```bash
  python3 cookie_converter.py --file "{cookies_file_path/x.com.cookies.json}" --username "{username}" --email "{email}"
  ```

### Fill in config

- First make a copy from the config templates

  ```bash
  cp ./config/token.json.template ./config/token.json
  cp ./config/monitoring.json.template ./config/monitoring.json
  ```

- Edit `config/token.json`

  1. Fill in `telegram_bot_token`

  2. Fill in `twitter_auth_username_list` according to your prepared Twitter account auth

  3. Now you can test whether the tokens can be used by
     ```bash
     python3 main.py check-tokens
     ```

- Edit `config/monitoring.json`

  (You need to fill in some telegram chat id here, you can get them from https://t.me/userinfobot and https://t.me/myidbot)

  1. If you need to view monitor health information (starting summary, daily summary, alert), fill in `maintainer_chat_id`

  2. Fill in one or more user to `monitoring_user_list`, and their notification telegram chat id, weight, which monitors to enable. The greater the weight, the higher the query frequency. The **profile monitor** is forced to enable (because it triggers the other 3 monitors), and the other 3 monitors are free to choose whether to enable or not

  3. You can check if your telegram token and chat id are correct by
     ```bash
     python3 main.py check-tokens --telegram_chat_id {your_chat_id}
     ```

### Run

```bash
python3 main.py run
```

|         Flag          | Default |                        Description                        |
| :-------------------: | :-----: | :-------------------------------------------------------: |
|      --interval       |   15    |                   Monitor run interval                    |
|       --confirm       |  False  |     Confirm with the maintainer during initialization     |
| --listen_exit_command |  False  | Liten the "exit" command from telegram maintainer chat id |
| --send_daily_summary  |  False  |         Send daily summary to telegram maintainer         |

## sync monitoring tweets to mangodb

1. add database.json to config file.

- database.json example:

```
{
    "development": {
    "mongodb_uri": "mongodb://localhost:27017/",
    "database": "twitter_monitor",
    "collections": {
      "tweets": "tweets",
      "user_metrics": "user_metrics",
      "analysis": "analysis"
    }
  }
}
```

2. add users_to_save.json to config file.

- users_to_save.json:

```
{
    "users": [
        {
            "username": "user to monitor"
        }
    ]
}
```

3. install mangodb https://www.mongodb.com/docs/manual/installation/

4. optional install mangoDB GUI https://www.mongodb.com/try/download/compass

### run

```
python3 run_tweet_saver.py
```

## Contact me

Telegram: [@ionic_bond](https://t.me/ionic_bond)

## Donate

[PayPal Donate](https://www.paypal.com/donate/?hosted_button_id=D5DRBK9BL6DUA) or [PayPal](https://paypal.me/ionicbond3)
