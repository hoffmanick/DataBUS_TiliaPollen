from bs4 import BeautifulSoup
import pandas as pd
import csv
import os

def get_text_or_none(parent, tag_name):
    tag = parent.find(tag_name)
    return tag.text.strip() if tag and tag.text else None
    
path = os.path.expanduser("~/Documents/lapd_tlx")
for file_name in os.listdir(path):
    full_path = os.path.join(path,file_name)
    if file_name.endswith(".tlx"):
        file_name_mod = file_name.removesuffix(".tlx")
        file_name_mod = file_name_mod.replace(" ","_")
        output_dir = os.path.join(path,"outputs/")
        print(file_name)
        with open(full_path, "r") as file:
            xml_content = file.read()

        soup = BeautifulSoup(xml_content, "xml")

        contacts_section = soup.find("Contacts")
        rows = []
        if contacts_section:
            for contact in contacts_section.find_all("Contact", recursive=False):
                contact_id = contact.get("ID")
                data = {"InternalContactID": contact_id}
                for child in contact.find_all(recursive=False):
                    if child.name == "Address":
                        address_lines = child.find_all("AddressLine")
                        data["Address"] = "\n".join(line.get_text(strip=True) for line in address_lines)
                    elif child.name == "Title":
                        data["ContactTitle"] = child.get_text(strip=True)
                    else:
                        data[child.name] = child.get_text(strip=True)
                rows.append(data)

        # Convert to DataFrame and CSV
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(output_dir, f"{file_name_mod}_contacts.csv"), index=False)


        publications = []
        authors = []

        # Only look at the main <Publications> section under <TiliaFile>
        publications_section = soup.find("TiliaFile").find("Publications")

        if publications_section:
            for pub in publications_section.find_all("Publication", recursive=False):
                pub_data = {
                    "PublicationID": pub.get("ID"),
                    "PrimaryPub": pub.get("Primary"),
                    "PublicationType": pub.find("PublicationType").get_text(strip=True) if pub.find("PublicationType") else None,
                    "NeotomaID_pub": pub.find("NeotomaID").get_text(strip=True) if pub.find("NeotomaID") else None,
                    "DOI_pub": pub.find("DOI").get_text(strip=True) if pub.find("DOI") else None,
                    "PublicationYear": pub.find("PublicationYear").get_text(strip=True) if pub.find("PublicationYear") else None,
                    "Citation": pub.find("Citation").get_text(strip=True) if pub.find("Citation") else None,
                    "Title": pub.find("Title").get_text(strip=True) if pub.find("Title") else None,
                    "Journal": pub.find("Journal").get_text(strip=True) if pub.find("Journal") else None,
                    "Volume": pub.find("Volume").get_text(strip=True) if pub.find("Volume") else None,
                    "Pages": pub.find("Pages").get_text(strip=True) if pub.find("Pages") else None,
                }
                publications.append(pub_data)

                # Extract associated authors
                for author in pub.find_all("Author"):
                    author_data = {
                        "PublicationID_Author": pub.get("ID"),
                        "ContactID": author.find("Contact").get("ID") if author.find("Contact") else None,
                        "LastName": author.find("LastName").get_text(strip=True) if author.find("LastName") else None,
                        "Initials": author.find("Initials").get_text(strip=True) if author.find("Initials") else None,
                    }
                    authors.append(author_data)

        # Convert to DataFrames
        pub_df = pd.DataFrame(publications)
        auth_df = pd.DataFrame(authors)

        # Save to CSV
        pub_df.to_csv(os.path.join(output_dir, f"{file_name_mod}_publications.csv"), index=False)
        auth_df.to_csv(os.path.join(output_dir, f"{file_name_mod}_authors.csv"), index=False)

        data_rows = {}

        for col in soup.find_all("Col"):
            col_id = col.get("ID")
    
            # Each cell within the column
            for cell in col.find_all("cell"):
                row_num = int(cell.get("row"))
        
                # Prefer <text>, fallback to <value>
                text_elem = cell.find("text")
                value_elem = cell.find("value")
                content = text_elem.get_text(strip=True) if text_elem else ""
                if not content and value_elem:
                    content = value_elem.get_text(strip=True)

                # Add to the correct row
                if row_num not in data_rows:
                    data_rows[row_num] = {}
                data_rows[row_num][col_id] = content

        sorted_rows = sorted(data_rows.items())
        column_ids = sorted({col.get("ID") for col in soup.find_all("Col")}, key=int)

        with open(os.path.join(output_dir, f"{file_name_mod}_data_spreadsheet.csv"), "w", newline="") as f:
            writer = csv.writer(f)
            # Header
            writer.writerow([f"Col_{cid}" for cid in column_ids])
    
            # Rows
            for _, row_data in sorted_rows:
                writer.writerow([row_data.get(cid, "") for cid in column_ids])


            site = soup.find("Site")
            site_data = {
                "SiteName": get_text_or_none(site, "SiteName"),
                "LongEast": get_text_or_none(site, "LongEast"),
                "LongWest": get_text_or_none(site, "LongWest"),
                "LatNorth": get_text_or_none(site, "LatNorth"),
                "LatSouth": get_text_or_none(site, "LatSouth"),
                "Altitude": get_text_or_none(site, "Altitude"),
                "Area": get_text_or_none(site, "Area"),
                "Country": get_text_or_none(site, "Country"),
                "State": get_text_or_none(site, "State"),
                "County": get_text_or_none(site, "County"),                  # Optional
                "SiteDescription": get_text_or_none(site, "SiteDescription"), # Optional
                "SiteNotes": get_text_or_none(site, "Notes"),
            }

            # Add Lake Parameters if present
            lake_params = site.find_all("LakeParameter")
            for i, lp in enumerate(lake_params, start=1):
                key = get_text_or_none(lp, "Parameter")
                val = get_text_or_none(lp, "Value")
                if key:
                    col_name = f"param_{key}"
                    site_data[col_name] = val

            pd.DataFrame([site_data]).to_csv(
    os.path.join(output_dir, f"{file_name_mod}_site.csv"), 
    index=False
)
        # 2. Extract CollectionUnit
        cu = soup.find("CollectionUnit")
        cu_data = {
            "Handle": cu.Handle.text.strip(),
            "CollectionName": get_text_or_none(cu, "CollectionName"),
            "CollectionType": get_text_or_none(cu, "CollectionType"),
            "CollectionDevice": get_text_or_none(cu, "CollectionDevice"),
            "CollectionDate": get_text_or_none(cu, "CollectionDate"),
            "CUNotes": get_text_or_none(cu, "Notes"),
            "DepositionalEnvironment": get_text_or_none(cu, "DepositionalEnvironment"),
            "WaterDepth": get_text_or_none(cu, "WaterDepth"),
            "Location": get_text_or_none(cu, "Location"),
            "GPSLat": get_text_or_none(cu, "GPSLat"),
            "GPSLong": get_text_or_none(cu, "GPSLong"),
            "GPSError": get_text_or_none(cu, "GPSError"),
            "GPSAltitude": get_text_or_none(cu, "GPSAltitude"),
            "Substrate": get_text_or_none(cu, "Substrate"),
            "SlopeAngle": get_text_or_none(cu, "SlopeAngle"),
            "SlopeAspect": get_text_or_none(cu, "SlopeAspect"),
            "Collectors": ",".join([c["ID"] for c in cu.find_all("Contact")])
        }
        pd.DataFrame([cu_data]).to_csv(os.path.join(output_dir, f"{file_name_mod}_collectionunit.csv"), index=False)

        # 3. Extract Dataset(s)
        datasets = []

        for ds in soup.find_all("Dataset"):
            data = {
                "DatasetType": ds.find("DatasetType").text.strip() if ds.find("DatasetType") else "",
                "Name": ds.find("Name").text.strip() if ds.find("Name") else "",
                "IsSSamp": ds.find("IsSSamp").text.strip() if ds.find("IsSSamp") else "",
                "WhitmoreData": ds.find("WhitmoreData").text.strip() if ds.find("WhitmoreData") else "",
                "IsAggregate": ds.find("IsAggregate").text.strip() if ds.find("IsAggregate") else "",
            }

            investigators = ds.find("Investigators")
            data["Investigators"] = ",".join(
                [c.get("ID") for c in investigators.find_all("Contact")]
            ) if investigators else ""

            processors = ds.find("Processors")
            data["Processors"] = ",".join(
                [c.get("ID") for c in processors.find_all("Contact")]
            ) if processors else ""

            publications = ds.find("Publications")
            data["Publications"] = ",".join(
                [p.get("ID") for p in publications.find_all("Publication")]
            ) if publications else ""

            datasets.append(data)

        # Save to CSV
        pd.DataFrame(datasets).to_csv(os.path.join(output_dir, f"{file_name_mod}_datasets.csv"), index=False)

        # 4. Extract GeochronDataset / GeochronSamples
        samples = []
        geo_ds = soup.find("GeochronDataset")

        if geo_ds:
            # Get all investigator IDs (could be multiple)
            investigator_ids = [
                c.get("ID") for c in geo_ds.find_all("Contact")
            ] if geo_ds.find("Investigators") else []
            investigator_ids_str = ",".join(investigator_ids)

            # Loop over each Geochronology block
            for geochronology in geo_ds.find_all("Geochronology"):
                analysis_unit_id = geochronology.get("AnalysisUnitID")

                # Loop over samples inside each Geochronology
                for sample in geochronology.find_all("GeochronSample"):
                    sample_data = {
                        "SampleID": sample.get("ID"),
                        "AnalysisUnitID": analysis_unit_id,
                        "Investigators_geochron": investigator_ids_str,
                        "Method": get_text_or_none(sample, "Method"),
                        "AgeUnits_Sample": get_text_or_none(sample, "AgeUnits"),
                        "Depth": get_text_or_none(sample, "Depth"),
                        "Thickness": get_text_or_none(sample, "Thickness"),
                        "LabNumber": get_text_or_none(sample, "LabNumber"),
                        "SampleAge": get_text_or_none(sample, "Age"),
                        "ErrorOlder": get_text_or_none(sample, "ErrorOlder"),
                        "ErrorYounger": get_text_or_none(sample, "ErrorYounger"),
                        "Sigma": get_text_or_none(sample, "Sigma"),
                        "StdDev": get_text_or_none(sample, "StdDev"),
                        "GreaterThan": get_text_or_none(sample, "GreaterThan"),
                        "MaterialDated": get_text_or_none(sample, "MaterialDated"),
                        "SampleNotes": get_text_or_none(sample, "Notes"),
                        "PublicationsText": get_text_or_none(sample, "PublicationsText")
                    }

                    # Publications — still works if they exist
                    publications = geo_ds.find("Publications")
                    sample_data["PublicationIDs"] = ",".join(
                        [p.get("ID") for p in publications.find_all("Publication")]
                    ) if publications else ""

                    # Parameters — dynamically add columns
                    parameters_container = sample.find("Parameters")
                    if parameters_container:
                        for param in parameters_container.find_all("Parameter"):
                            name_text = get_text_or_none(param, "Name")
                            value_text = get_text_or_none(param, "Value")
                            if name_text:
                                col_name = f"param_{name_text}"
                                sample_data[col_name] = value_text

                    samples.append(sample_data)

        # Save
        pd.DataFrame(samples).to_csv(
            os.path.join(output_dir, f"{file_name_mod}_geochrondataset.csv"),
            index=False
        )

        age_models = []
        chroncontrols = []

        for age_model_idx, age_model in enumerate(soup.find_all('AgeModel') or [], start=1):
            # Give each AgeModel a unique ID (could also use ChronNumber if guaranteed unique)
            age_model_id = age_model_idx

            # AgeModels table
            age_models.append({
                'AgeModelID': age_model_id,
                'ChronNumber': get_text_or_none(age_model, 'ChronNumber'),
                'ChronologyName': get_text_or_none(age_model, 'ChronologyName'),
                'AgeUnits': get_text_or_none(age_model, 'AgeUnits'),
                'Default': get_text_or_none(age_model, 'Default'),
                'DatePrepared': get_text_or_none(age_model, 'DatePrepared'),
                'Model': get_text_or_none(age_model, 'Model'),
                'AgeBoundOlder': get_text_or_none(age_model, 'AgeBoundOlder'),
                'AgeBoundYounger': get_text_or_none(age_model, 'AgeBoundYounger'),
                'AgeModelNotes': get_text_or_none(age_model, 'Notes'),
                'PreparersText': get_text_or_none(age_model, 'PreparersText'),
                'PreparersContactID': (
                    age_model.Preparers.Contact.get('ID')
                    if age_model.find('Preparers') and age_model.find('Preparers').find('Contact')
                    else None
                )
            })

            # ChronControls table
            for cc in age_model.find_all('ChronControl') or []:
                # Grab all GeochronLink IDs (may be multiple)
                geochron_ids = [
                    link.get('ID')
                    for link in cc.find_all('GeochronLink')
                    if link.get('ID') is not None
                ]
                geochron_ids_str = ",".join(geochron_ids) if geochron_ids else None

                chroncontrols.append({
                    'AgeModelID': age_model_id,  # foreign key
                    'ControlType': get_text_or_none(cc, 'ControlType'),
                    'Depth': get_text_or_none(cc, 'Depth'),
                    'Thickness': get_text_or_none(cc, 'Thickness'),
                    'AgeUnits': get_text_or_none(cc, 'AgeUnits'),
                    'ChronControlAge': get_text_or_none(cc, 'Age'),
                    'AgeLimitOlder': get_text_or_none(cc, 'AgeLimitOlder'),
                    'AgeLimitYounger': get_text_or_none(cc, 'AgeLimitYounger'),
                    'ChronControlNotes': get_text_or_none(cc, 'Notes'),
                    'GeochronLinkIDs': geochron_ids_str
                })



        pd.DataFrame(age_models).to_csv(
            os.path.join(output_dir, f"{file_name_mod}_agemodels.csv"),
            index=False
        )
        pd.DataFrame(chroncontrols).to_csv(
            os.path.join(output_dir, f"{file_name_mod}_chroncontrols.csv"),
            index=False
        )