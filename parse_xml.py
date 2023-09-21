# -*- coding: utf-8 -*-

import xml.etree.cElementTree as ET
import pandas as pd
import os


def get_namespaces(xml_file):
    namespaces = {}
    for _, elem in ET.iterparse(xml_file, events=('start-ns',)):
        prefix, ns_uri = elem
        namespaces[prefix] = '{' + ns_uri + '}'
    return namespaces


def print_tree(element, indent=0):
    """Print the tree with indentation"""
    print(' ' * indent + element.tag)
    for child in element:
        print_tree(child, indent + 4)


def get_unique_tags(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    unique_tags = set()

    for elem in root.iter():
        unique_tags.add(elem.tag)

    return unique_tags


def check_for_registered_name(node, ns):
    """For a given node, check whether "RegisteredName" is one of the children
    """
    name_present = 0
    for child in node:
        name_present += len(child.findall(ns + 'RegisteredName'))
    return bool(name_present)


def candidate_present(node, ns):
    """For a given node, check whether "Candidate" is one of the children
    """
    for child in node:
        if child.tag == ns + 'Candidate':
            return True
    return False


def get_relevant_data(root_node, namespace, all_candidates=False):
    """Deze functie haalt de relevante data uit de parsed etree, en stopt het in een python nested dictionary. 
    
    De input voor de functie is wat ik nu de root node hebt genoemd, dus het hoogste deel van de tree. 
    Dat is in mijn text.xml file "ReportingUnitVotes", maar het leek bij jou dat er nog hogere nodes zijn 
    in de tree (ik neem aan dat er bijvoorbeeld meerdere stembureaus zijn). Je kan deze functie gebruiken als je 
    in plaats van de parsed_xml.getroot() de relevante node "ReportingUnitVotes" als input invoert. 
    
    De strategie hier: er zijn twee soorten nodes onder de root: ReportingUnitIdentifier en Selection. 
    """
    data = {}
    data['IDs'] = []
    n_candidates_processed = 0
    for node in root_node:
        if node.tag == namespace + 'ReportingUnitIdentifier':
            data['ReportingUnitIdentifier'] = node.attrib.get('Id')
            data['ReportingUnitName'] = node.text
        elif node.tag == namespace + 'Selection':

            # check: does this Selection node contain a registered name?  # if not, skip to next Selection node
            name_present = check_for_registered_name(node, namespace)
            # if name is present, this is the node defining the party. if not it will be a specific candidate (I think)
            if name_present:
                n_candidates_processed = 0
                for child in node:
                    if child.tag == namespace + 'AffiliationIdentifier':
                        affiliation_id = int(child.attrib.get('Id'))
                        data[affiliation_id] = {}
                        data['IDs'].append(affiliation_id)
                        # data[affiliation_id]['AffiliationIdentifier'] = child.attrib.get('Id')
                        data[affiliation_id]['RegisteredName'] = child[0].text
                    if child.tag == namespace + 'ValidVotes':
                        data[affiliation_id]['ValidVotes'] = child.text
            elif candidate_present(node, ns):
                # if its a candidate, we write each candidate to the dictionary corresponding to the party
                if not all_candidates:
                    if n_candidates_processed > 0:
                        continue
                for child in node:
                    if child.tag == namespace + 'Candidate':
                        candidate_id = int(child[0].attrib.get('Id'))
                        data[affiliation_id][candidate_id] = {}
                    elif child.tag == namespace + 'ValidVotes':
                        data[affiliation_id][candidate_id]['ValidVotes'] = child.text
                n_candidates_processed += 1
    return data


def check_vote_totals():
    # check that the sum of the votes for each party is equal to the total number of votes
    total_votes = 0
    for party in data['IDs']:
        total_votes += int(data[party]['ValidVotes'])
        candidate_votes = 0
        for candidate in data[party].keys():
            if not isinstance(candidate, int):
                continue
            candidate_votes += int(data[party][candidate]['ValidVotes'])
        assert int(data[party]['ValidVotes']) == candidate_votes


def get_dataframe(data):
    reporting_unit_identifier = data['ReportingUnitIdentifier']
    reporting_unit_name = data['ReportingUnitName']

    flattened_data = []

    for party_number, party_data in data.items():
        if isinstance(party_number, int):
            total_votes_for_party = int(party_data['ValidVotes'])  # Extracting total votes for the party
            for cand_number, vote_data in party_data.items():
                if isinstance(cand_number, int):
                    entry = {
                        'ReportingUnitIdentifier': reporting_unit_identifier,
                        'ReportingUnitName': reporting_unit_name,
                        'PartyNumber': party_number,
                        'PartyName': party_data['RegisteredName'],
                        'CandidateID': cand_number,
                        'Votes': int(vote_data['ValidVotes']),
                        'TotalVotesForParty': total_votes_for_party
                    }
                    flattened_data.append(entry)

    # Convert to DataFrame
    df = pd.DataFrame(flattened_data)

    # Set multi-index
    df.set_index(['ReportingUnitName', 'PartyNumber'], inplace=True)

    return df


if __name__ == "__main__":
    from tqdm import tqdm
    import glob

    all_candidates = False  # if True, all candidates are included. If False, only lijsttrekkers

    path = os.getcwd()

    xml_files = glob.glob(os.path.join(path, '*.xml'))

    all_data = pd.DataFrame({})

    for council_file in tqdm(xml_files, desc='Gemeente'):

        full_path = os.path.join(path, council_file)
        name_spaces = get_namespaces(full_path)
        ns = name_spaces['']  # the default namespace

        # parse the XML file using an element tree
        parsed_xml = ET.parse(full_path)

        election = parsed_xml.getroot().find(ns + "Count").find(ns + "Election")
        contest = election.find(ns + "Contests").find(ns + "Contest")

        stembureaus = contest.findall(ns + 'ReportingUnitVotes')

        dataframes = []
        for s in tqdm(stembureaus, leave=False, desc='Stembureau'):
            data = get_relevant_data(s, ns, all_candidates=all_candidates)

            if all_candidates:
                check_vote_totals()  # checken of het totale aantal stemmen klopt per partij (= som van alle kandidaten)

            df = get_dataframe(data)
            dataframes.append(df.reset_index())

        try:
            council_data = pd.concat(dataframes, ignore_index=True)
        except ValueError:
            tqdm.write('Could not find data in file: ' + council_file)

        # extract naam van de gemeente
        council_name = council_file.split('_')[-1].split(".")[0]
        council_data['CouncilName'] = council_name

        all_data = pd.concat([all_data, council_data], ignore_index=True)

    all_data.to_csv('processed_data.csv')
