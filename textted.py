import os
import zipfile
import re
import sys
import fnmatch
import json
from collections import defaultdict
from datetime import datetime


START_DOC = re.compile(r'^(\d+\.\d+)/(\d+)')
START_SECTION = re.compile(r'^([A-Z]{2}): ')
START_SUBSECTION = re.compile(r'^\s*(\d+)\.')
START_SUBSUBSECTION = re.compile(r'^\s*[a-z]\)')

MAPPING = {
    'TX_11': 'contract_additional_information',
    'TX_1': 'contract_authority_address',
    'CY': 'contract_authority_country',
    'TW': 'contract_authority_town',
    'PC': 'contract_cpv_code',
    'TX_5': 'contract_offers_received_num',
    'DI': 'document_directive',
    'DS': 'document_dispatch_date',
    'HD': 'document_heading',
    'OL': 'document_orig_language',
    'AU': 'document_authority_name',
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

    def parse_TX(self, data):
        if not self.should_parse(self.doc):
            return {'TX': '\n'.join(data)}
        subsections = defaultdict(list)
        subsection_name = None
        for line in data:
            line = line.strip()
            match = START_SUBSECTION.search(line)
            if match is not None:
                subsection_name = match.group(1)
                line = line[len(subsection_name):]
                subsection_name = 'TX_%s' % subsection_name
                subsections[subsection_name].append(line)
            else:
                if not subsection_name:
                    subsection_name = 'TX_'
                subsections[subsection_name].append(line)
        result = {}
        for subsection in subsections:
            subsection_data = subsections[subsection]
            if hasattr(self, 'parse_TX_%s' % subsection):
                method = getattr(self, 'parse_TX_%s' % subsection)
                result.update(method(subsection_data))
            else:
                result.update({subsection: '\n'.join(subsection_data)})
        return result

    def parse_TX_1(self, data):
        try:
            data[0] = data[0].split(':', 1)[1]
        except ValueError:
            pass
        return {
            'awarding_authority': ' '.join(data)
        }

    def parse_TX_10(self, data):
        assert len(data) == 1
        data = data[0]
        if ':' not in data:
            return {'notice_published': ''}
        raw_date = data.split(':', 1)[1]
        raw_date = raw_date.replace(' ', '').strip()
        if raw_date.endswith('.'):
            raw_date = raw_date[:-1].strip()
        try:
            return {'notice_published': datetime.strptime(raw_date, '%d.%m.%Y').strftime('%Y-%m-%d')}
        except ValueError:
            return {'notice_published': ''}


def get_zip_files(path):
    for root, dirnames, filenames in os.walk(path):
        for filename in fnmatch.filter(filenames, 'EN*.ZIP'):
            yield os.path.join(root, filename)


def get_text(path):
    zf = zipfile.ZipFile(path)
    files = zf.namelist()
    return zf.read(files[0]).decode('latin1')


def main(path):
    first = True
    parser = TextTedParser(filters={'document_document_type_code': '7'})
    sys.stdout.write('[')
    for filename in get_zip_files(path):
        for doc in parser.get_docs(get_text(filename)):
            # print filename
            # print '&' * 20
            if first:
                first = False
            else:
                sys.stdout.write(',')
            sys.stdout.write(json.dumps(doc))
            # print '&' * 20
    sys.stdout.write(']')

if __name__ == '__main__':
    main(*sys.argv[1:])
