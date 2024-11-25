import json
import argparse


def convert_cookies(file_path, username, email):
    with open(file_path, 'r') as f:
        cookies = json.load(f)

    fields = ['guest_token', 'ct0', 'auth_token']

    new_cookies = {}

    for field_data in cookies:
        if field_data['name'] in fields:
            new_cookies[field_data['name']] = field_data['value']

    new_cookies['email'] = email

    print(new_cookies)

    with open(f'cookies/{username}.json', 'w') as f:
        json.dump(new_cookies, f, indent=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, required=True)
    parser.add_argument('--username', type=str, required=True)
    parser.add_argument('--email', type=str, required=True)
    args = parser.parse_args()
    convert_cookies(args.file, args.username, args.email)
