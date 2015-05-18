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
START_TX_SECTION = re.compile(r'SECTION ([IVX]+):')
START_TX_SUBSECTION = re.compile(r'^\s*([IVX]+\.\d+)\)')
START_SUBSUBSECTION = re.compile(r'^\s*[a-z]\)')
TI_RE = re.compile(r'^\s*(\w{1,2})-(\w+): (.*)')
CONTRACTOR_ADDRESS = re.compile(r'(.+), ?([A-Z]{1,2})\-([A-Z0-9]+) +(.{2,})$')
AMOUNT = r'((?:[\d ])*(?:\.,\d{1,2})?) ?([A-Z]{2,3})'
AMOUNT_RE = re.compile(AMOUNT)
VALUE_RE = re.compile(r'^Value: %s\.?' % AMOUNT)
VAT_RE = re.compile(r'VAT rate \(%\) ([\d\.,]+)\b')

PRICE_RES = [
    re.compile(r'(?P<value>(\d+\s)+(?:[\.\,]\d+)?) ?(?P<currency>[A-Z]{,3})?'),
    re.compile(r'(?P<currency>[A-Z]{2,3})? (?P<value>(?:\d+\s)+(?:[\.\,]\d+)?)'),
]

MAPPING = {
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
        return ' '.join(data).strip()

    def _parse_date(self, data):
        data = ' '.join(data)
        raw_date = data.replace(' ', '').strip()
        if raw_date.endswith('.'):
            raw_date = raw_date[:-1].strip()
        try:
            return datetime.strptime(raw_date, '%d.%m.%Y').strftime('%Y-%m-%d')
        except ValueError:
            return ''

    def _parse_bool(self, val):
        if val in ('Yes',):
            return True
        if val in ('No',):
            return False
        return None

    def _parse_int(self, val):
        val = val.replace('.', '').strip()
        try:
            return int(val)
        except ValueError:
            return None

    def _parse_amount(self, value):
        value = value.replace(' ', '')
        value = re.sub(r'[\.,](\d{3})', '\\1', value)
        value = value.replace(',', '.')
        try:
            return float(value)
        except ValueError:
            return None

    def _parse_money(self, value):
        match = AMOUNT_RE.search(value)
        if match is None:
            return None
        return {
            'value': self._parse_amount(match.group(1)),
            'currency': match.group(2)
        }

    def _parse_address(self, data):
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
            'contract_operator_official_name': data,
            'contract_operator_address': '',
            'contract_operator_postal_code': '',
            'contract_operator_town': '',
            'contract_operator_country': ''
        }

    def _parse_contract_value(self, data, possible_values):
        values = defaultdict(dict)
        current_key = None
        for line in data:
            matches = [k for k, v in possible_values.items() if v in line]
            if matches:
                current_key = matches[0]
            if current_key:
                value_match = VALUE_RE.search(line)
                if value_match is not None:
                    money_value = self._parse_amount(value_match.group(1))
                    values[current_key]['value'] = money_value
                    currency = value_match.group(2)
                    values[current_key]['currency'] = currency
                    continue
                if 'Excluding VAT' in line:
                    values[current_key]['vat'] = False
                    continue
                vat_match = VAT_RE.search(line)
                if vat_match is not None:
                    values[current_key]['vat'] = self._parse_amount(vat_match.group(1))
                    continue
        return values

    def parse_TI(self, data):
        match = TI_RE.search(data[0])
        if match:
            return {
                'document_title_country': match.group(1),
                'document_title_text': ('%s %s' % (
                    match.group(3), ' '.join(data[1:]))).strip(),
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

    def parse_OT(self, data):
        # Original language text, discard
        return {}

    def parse_CO(self, data):
        data = self._parse_simple_value(data)
        return self._parse_address(data)

    def parse_TX(self, data):
        if not self.should_parse(self.doc):
            return {'TX': '\n'.join(data)}

        full_text = '\n'.join(data)
        if len(START_TX_SECTION.findall(full_text)) > 1:
            return self.parse_new_TX(data)
        return self.parse_old_TX(data)

    def parse_new_TX(self, data):
        subsections = defaultdict(list)
        section = None
        subsection = None
        result = {}
        for line in data:
            line = line.strip()
            match = START_TX_SECTION.search(line)
            if match is not None:
                section = match.group(1)
                continue
            if section is None:
                continue
            match = START_TX_SUBSECTION.search(line)
            if match is not None:
                if subsection is not None:
                    self._run_parser_new_TX(
                        subsections[subsection],
                        subsection,
                        result
                    )
                    subsections[subsection] = []
                subsection = match.group(1)
                line = line[len(match.group(0)):]
                subsections[subsection].append(line)
                continue
            if subsection is None:
                continue
            subsections[subsection].append(line)

        if subsection is not None:
            self._run_parser_new_TX(
                subsections[subsection],
                subsection,
                result
            )
        return result

    def _run_parser_new_TX(self, data, subsection, result):
        method_name = 'parse_new_%s' % subsection.replace('.', '_')
        if hasattr(self, method_name):
            method = getattr(self, method_name)
            new_result = method(data)
            for key in new_result:
                if key not in result:
                    result[key] = []
                result[key].append(new_result[key])
        else:
            result.update({'TX_' + subsection: '\n'.join(data)})
        return result

    def parse_new_V_1(self, data):
        return {
            'contract_awarded_date': self._parse_date(self._parse_simple_value(data)),
        }

    def parse_new_V_2(self, data):
        return {
            'contract_offers_received_num': self._parse_int(self._parse_simple_value(data)),
        }

    def parse_new_V_5(self, data):
        return {
            'subcontractors': self._parse_bool(self._parse_simple_value(data))
        }

    def parse_new_V_4(self, data):
        possible_values = {
            'contract_initial_value': 'Initial estimated total value',
            'contract_total_value': 'Total final value'
        }
        values = self._parse_contract_value(data, possible_values)
        res = {}
        for key in possible_values:
            if key in values:
                value = values[key]
                if 'value' in value:
                    res[key + '_currency'] = value['currency']
                    res[key + '_cost'] = value['value']
                    if value['currency'] == 'EUR':
                        res[key + '_cost_eur'] = value['value']
                    if 'vat' in value:
                        if value['vat'] is False:
                            res[key + '_vat_included'] = False
                            res[key + '_vat_rate'] = None
                        else:
                            res[key + '_vat_included'] = True
                            res[key + '_vat_rate'] = value['vat']
        return res

    def _get_label_method(self, label):
        LABEL_MAPPING = {
            'Number of tenders received': 'contract_offers_received_num',
            'Contract number and value': 'contract_value',
            'Date of award of the contract': 'contract_awarded_date',
            'Name, address and nationality of successful tenderer': 'contract_operator',
            'Contracting authority': 'contracting_authority'
        }
        for needle in LABEL_MAPPING:
            if needle in label:
                return getattr(self, 'parse_old_%s' % LABEL_MAPPING[needle])
        return None

    def parse_old_TX(self, data):
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
            label_method = self._get_label_method(subsection_data[0])
            if label_method is not None:
                data = self._parse_simple_value(subsection_data)
                result.update(label_method(data))
            else:
                result.update({subsection: subsection_data})
        return result

    def parse_old_contract_operator(self, data):
        return self._parse_address(data)

    def parse_old_contract_awarded_date(self, data):
        return {
            'contract_awarded_date': self._parse_date(data)
        }

    def parse_old_contract_value(self, data):
        result = self._parse_money(data)
        if result is None:
            return {
                'contract_total_value_cost': None
            }
        return {
            'contract_total_value_cost': result['value'],
            'contract_total_value_currency': result['currency'],
        }

    def parse_old_contract_offers_received_num(self, data):
        return {
            'contract_offers_received_num': self._parse_int(data)
        }

    def parse_old_contracting_authority(self, data):
        return {
            'contract_authority_official_name': self._parse_simple_value(data)
        }


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
