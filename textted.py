import os
import zipfile
import re
import sys
import json
from collections import defaultdict
from datetime import datetime


START_DOC = re.compile(r'^(\d+\.\d+)/(\d+)')
START_SECTION = re.compile(r'^([A-Z]{2}): ')
START_SUBSECTION = re.compile(r'^\s*(\d{1,2})\.')
START_SUBSUBSECTION = re.compile(r'^\s*[a-z]\)')
TI_RE = re.compile(r'^\s*(\w{1,2})-(\w+): (.*)')
CONTRACTOR_ADDRESS = re.compile(r'(.+), ?([A-Z]{1,2})\-([A-Z0-9]+) +(.{2,})$')

PRICE_RES = [
    re.compile(r'(?P<value>(\d+\s)+(?:[\.\,]\d+)?) ?(?P<currency>[A-Z]{,3})?'),
    re.compile(r'(?P<currency>[A-Z]{2,3})? (?P<value>(?:\d+\s)+(?:[\.\,]\d+)?)'),
]

MAPPING = {
    'TX_11': 'contract_additional_information',
    'TX_1': 'contract_authority_address',
    'CY': 'contract_authority_country',
    'TW': 'contract_authority_town',
    'DI': 'document_directive',
    'DS': 'document_dispatch_date',
    'HD': 'document_heading',
    'OL': 'document_orig_language',
    'AU': 'document_authority_name',
    'DI': 'document_directive',
}


class TextTedParser(object):
    def __init__(self, filters=None):
        if filters is None:
            filters = {}
        self.filters = filters

    def finalize_doc(self):
        if self.doc is None:
            return None
        if not self.should_parse(self.doc):
            return None
        doc = self.apply_mapping(self.doc)
        return self.doc

    def apply_mapping(self, doc):
        res = {}
        for k in doc:
            if k in MAPPING:
                res[MAPPING[k]] = doc[k]
            else:
                res[k] = doc[k]
        return res

    def should_parse(self, doc):
        for f in self.filters:
            if doc[f] != self.filters[f]:
                return False
        return True

    def get_docs(self, text):
        self.section = None
        self.section_data = []
        self.doc = None
        for line in text.splitlines():
            match = START_DOC.search(line)
            if match is not None:
                self.close_section()
                doc = self.finalize_doc()
                if doc:
                    yield doc
                self.doc = {
                    '_doc_version': match.group(1),
                    '_doc_id': match.group(2),
                }
                continue
            if self.doc is None:
                continue
            if not line.strip():
                continue
            match = START_SECTION.search(line)
            if match is not None:
                if self.section:
                    self.close_section()
                self.section = match.group(1)
                self.section_data.append(line[4:].strip())
            else:
                if self.section:
                    if line.startswith(' ' * 4):
                        self.section_data.append(line[4:].strip())
                    else:
                        self.section_data[-1] += line.strip()
                else:
                    raise Exception('Indentation without section!')
        self.close_section()
        doc = self.finalize_doc()
        if doc:
            yield doc

    def close_section(self):
        if self.section is None:
            return
        if hasattr(self, 'parse_%s' % self.section):
            result = getattr(self, 'parse_%s' % self.section)(self.section_data)
        else:
            result = {self.section: '\n'.join(self.section_data)}
        self.doc.update(
            result
        )
        self.section = None
        self.section_data = []

    def _split_code(self, data):
        return [x.strip() for x in data[0].split('-', 1)]

    def _parse_simple_value(self, data):
        try:
            data[0] = data[0].split(':', 1)[1]
        except (ValueError, IndexError):
            pass
        return ' '.join(data)

    def parse_TI(self, data):
        match = TI_RE.search(data[0])
        if match:
            return {
                'document_title_country': match.group(1),
                'document_title_text': '%s %s' % (
                    match.group(3), ' '.join(data[1:])),
                'document_title_town': match.group(2)
            }
        return {
            'TI': '\n'.join(data)
        }

    def parse_PC(self, data):
        return {
            'contract_cpv_code': data[0].strip(),
            'document_cpvs': ','.join(data)
        }

    def parse_AA(self, data):
        d = self._split_code(data)
        return {
            'document_authority_type_code': d[0],
            'document_authority_type': d[1]
        }

    def parse_AC(self, data):
        d = self._split_code(data)
        return {
            'document_award_criteria_code': d[0],
            'document_award_criteria': d[1]
        }

    def parse_TY(self, data):
        d = self._split_code(data)
        return {
            'document_bid_type_code': d[0],
            'document_bid_type': d[1]
        }

    def parse_NC(self, data):
        d = self._split_code(data)
        return {
            'document_contract_nature_code': d[0],
            'document_contract_nature': d[1]
        }

    def parse_TD(self, data):
        d = self._split_code(data)
        return {
            'document_document_type_code': d[0],
            'document_document_type': d[1]
        }

    def parse_PR(self, data):
        d = self._split_code(data)
        return {
            'document_procedure_code': d[0],
            'document_procedure': d[1]
        }

    def parse_RP(self, data):
        d = self._split_code(data)
        return {
            'document_regulation_code': d[0],
            'document_regulation': d[1]
        }

    def parse_RC(self, data):
        data = [d.strip() for d in data if d.strip()]
        return {
            'document_orig_nuts_code': data[0]
        }

    def parse_OJ(self, data):
        date = data[0].split('/')
        return {
            'document_oj_collection': date[0],
            'document_oj_date': date[1]
        }

    def parse_CO(self, data):
        data = self._parse_simple_value(data)
        match = CONTRACTOR_ADDRESS.search(data)
        if match is not None:
            name_address = match.group(1)
            country = match.group(2)
            postcode = match.group(3)
            town = match.group(4)
            if ',' in name_address:
                name, address = name_address.split(',', 1)
            else:
                name = name_address
                address = ''

            return {
                'contract_operator_official_name': name,
                'contract_operator_address': address,
                'contract_operator_postal_code': postcode,
                'contract_operator_town': town,
                'contract_operator_country': country,
            }
        return {
            'contract_operator_official_name': '',
            'contract_operator_address': '',
            'contract_operator_postal_code': '',
            'contract_operator_town': '',
            'contract_operator_country': ''
        }

    def parse_TX(self, data):
        if not self.should_parse(self.doc):
            return {'TX': '\n'.join(data)}
        subsections = defaultdict(list)
        subsection_name = None
        next_subsection_no = 1
        for line in data:
            line = line.strip()
            match = START_SUBSECTION.search(line)
            if match is not None and int(match.group(1)) == next_subsection_no:
                subsection_name = match.group(1)
                line = line[len(subsection_name):]
                subsection_name = 'TX_%s' % subsection_name
                subsections[subsection_name].append(line)
                next_subsection_no += 1
            else:
                if not subsection_name:
                    subsection_name = 'TX_'
                subsections[subsection_name].append(line)
        result = {}
        for subsection in subsections:
            subsection_data = subsections[subsection]
            if hasattr(self, 'parse_%s' % subsection):
                method = getattr(self, 'parse_%s' % subsection)
                result.update(method(subsection_data))
            else:
                result.update({subsection: '\n'.join(subsection_data)})
        return result

    def parse_TX_1(self, data):
        return {
            'awarding_authority': self._parse_simple_value(data)
        }

    def parse_TX_5(self, data):
        return {
            'contract_offers_received_num': self._parse_simple_value(data)
        }

    def _parse_price(self, price):
        return float(price.replace(' ', '').replace(',', '.'))

    def parse_TX_8(self, data):
        prices = []
        for price_re in PRICE_RES:
            prices = price_re.finditer(' '.join(data))
            for m in prices:
                price = m.group('value')
                currency = m.group('currency')
                price = self._parse_price(price)
                prices.append((price, currency))
        return {
            'contract_total_value_cost': '',
            'contract_total_value_cost_eur': '',
            'contract_total_value_currency': '',
            'contract_total_value_high': '',
            'contract_total_value_high_eur': '',
            'contract_total_value_low': '',
            'contract_total_value_low_eur': '',
            'contract_total_value_vat_included': '',
        }

    def _parse_date(self, data, key):
        data = ' '.join(data)
        if ':' not in data:
            return {key: ''}
        raw_date = data.split(':', 1)[1]
        raw_date = raw_date.replace(' ', '').strip()
        if raw_date.endswith('.'):
            raw_date = raw_date[:-1].strip()
        try:
            return {key: datetime.strptime(raw_date, '%d.%m.%Y').strftime('%Y-%m-%d')}
        except ValueError:
            return {key: ''}

    def parse_TX_11(self, data):
        return self._parse_date(data, 'tx_11_notice_published')

    def parse_TX_12(self, data):
        return self._parse_date(data, 'tx_12_notice_postmarked')

    def parse_TX_13(self, data):
        return self._parse_date(data, 'tx_13_notice_received')


def check_file(filename):
    f = filename.lower()
    if '/en/' not in f:
        return False
    if not f.endswith('.zip'):
        return False
    if 'meta' in f:
        return False
    both_encodings = ['/%s/' % x in f for x in range(2005, 2008)]
    both_encodings.extend(['/2004-06/', '/2004-07/', '/2004-08/', '/2004-09/',
                           '/2004-10/', '/2004-11/', '/2004-12/'])
    if any(both_encodings) and 'iso' in f:
            return False
    return True


def get_zip_files(paths):
    for path in paths:
        for root, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filepath = os.path.join(root, filename)
                if not check_file(filepath):
                    continue
                yield filepath


def get_text(path):
    zf = zipfile.ZipFile(path)
    files = zf.namelist()
    codec = 'latin1'
    if 'utf-8' in path.lower() or 'utf8' in path.lower():
        codec = 'utf-8'
    return zf.read(files[0]).decode(codec)


def get_docs(paths, filters=None):
    parser = TextTedParser(filters=filters)
    for filename in get_zip_files(paths):
        for doc in parser.get_docs(get_text(filename)):
            doc['filename'] = filename
            yield doc


def main(*paths):
    first = True
    docs = get_docs(paths, {'document_document_type_code': '7'})
    sys.stdout.write('[')
    for doc in docs:
        # print '&' * 20
        if first:
            first = False
        else:
            # pass
            sys.stdout.write(',')
        sys.stdout.write(json.dumps(doc))
        # print json.dumps(doc, indent=4)
        # print '&' * 20
    sys.stdout.write(']')

if __name__ == '__main__':
    main(*sys.argv[1:])
