# OpenTED - Text Parser

```
# Get the data
wget -c -m ftp://guest:guest@ftp.ted.europa.eu/monthly-packages/
# Parse it to JSON
python textted.py ftp.ted.europa.eu/monthly-packages/ > ted_data.json

```

Major Features still missing:
 - parsing prices with lots
 - converting prices to euros
 - parsing multiple suppliers
