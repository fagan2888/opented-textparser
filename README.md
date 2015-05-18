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


Availability of TED data:

 - 1993 - 2004: latin1 coded text files
 - 2005 - 2007: latin1 and utf8 coded text files
 - 2008 - 2010: utf8 coded text files and meta-xml
 - 2011: only meta xml
 - 2012 - now: TED-XML
