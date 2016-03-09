# bdu_migration

## Required Packages

MySQLdb and progressbar

## Usage

python migrate.py --[Options] <path-to-data_file>

Example: python migrate.py --create-accounts data.csv

[Options]

```bash
--create-accounts	create an account for all existing users on Moodle
```
```bash
--create-unique-from-duplicate		create a single account for a set of duplicate records
```
```bash
--link-to-google	link existing accounts to google
```
```bash
--link-to-linkedin	link existing account to linkedin
```
```bash
--link-to-yahoo	link existing account to yahoo
```
```bash
--link-to-facebook	link existing account to facebook
```

Note: The data file content must contain lines of records with ';' as the delimiter between columns.

## Support

Python 2.7 & 3 supported
