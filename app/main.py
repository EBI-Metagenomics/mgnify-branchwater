import os
import yaml
from flask import Flask, render_template, request, jsonify
from functions import *
import json
import glob
import urllib3

import sentry_sdk
from sentry_sdk.integrations.pymongo import PyMongoIntegration
sentry_sdk.init(
    os.environ.get("SENTRY_DSN"),
    enable_tracing=True,
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
    integrations=[
        PyMongoIntegration(),
    ],
)

app = Flask(__name__)  # create flask/app instance
# may not be needed/not yet integrated
app.config['SECRET_KEY'] = 'my-secret-key'

# Load configuration from config.yaml
with open('config.yml', 'r') as file:
    config_data = yaml.safe_load(file)

    app.config.update(config_data)

KSIZE = app.config.get('ksize', 21)
THRESHOLD = app.config.get('threshold', 0.1)
print(f'ksize: {KSIZE}')
print(f'threshold: {THRESHOLD}')

# define '/' and 'home' route

# @app.route('/', methods=['GET', "POST"])
# @app.route('/home', methods=['GET', "POST"])
# def home():
#     if request.method == 'POST':
#         accession = request.args.get('accession')
#         jsonify(accession)
#         sketch_dir = 'signatures'
#         pattern = os.path.join(sketch_dir, f"{accession}.fna.sig")
#         directory_path = 'signatures'
#
#         # if os.path.isdir(directory_path):
#         #     return 'HELLO'
#         # else:
#         #     return 'WORLD'
#         # return glob.glob('M')
#         matching_files = glob.glob(pattern)
#
#         if not matching_files:
#             return jsonify({'error': f'No .sig file found for accession: {accession}'}), 404
#
#         sig_file_path = matching_files[0]  # Use the first match
#
#         try:
#             # Load sourmash signature from file
#             # with open('MGYG000304657.fna.sig', 'r') as f:
#             with open(sig_file_path, 'r') as f:
#                 signature_data = json.load(f)
#         except (FileNotFoundError, json.JSONDecodeError):
#             return jsonify({'error': 'Could not read or parse the signature file'}), 400
#
#         try:
#             # Call getacc with the loaded signature
#             mastiff_df = getacc(signature_data, app.config)
#         except SearchError as e:
#             return jsonify({'error': str(e)}), 500
#
#         print(f"SANITY CHECK: {len(mastiff_df)}")
#
#         acc_t = tuple(mastiff_df.SRA_accession.tolist())
#
#         meta_list = (
#             'bioproject', 'assay_type', 'collection_date_sam',
#             'geo_loc_name_country_calc', 'organism', 'lat_lon'
#         )
#
#         result_list = getmongo(acc_t, meta_list, app.config)
#         print(f"Metadata for {len(result_list)} acc returned.")
#
#         mastiff_dict = mastiff_df.to_dict('records')
#
#         for r in result_list:
#             for m in mastiff_dict:
#                 if r['acc'] == m['SRA_accession']:
#                     r['containment'] = round(m['containment'], 2)
#                     r['cANI'] = round(m['cANI'], 2)
#                     break
#
#         return jsonify(result_list)
#
#     return render_template('index.html')

@app.route('/health', methods=["GET"])
def check_health():
    base_url = 'http://index-service'
    # base_url = 'http://localhost:80clear83'
    http = urllib3.PoolManager()
    r = http.request('GET',
                     f"{base_url}/health",
                     headers={'Content-Type': 'application/json'})

    print(f"Health status: {r.status}")

    if r.status != 200:
        raise SearchError(r.data.decode('utf-8'), r.status)

@app.route('/mags', methods=["POST"])
def search_by_mgyg_accession():
    if request.method == 'POST':
        accession = request.args.get('accession')
        catalogue = request.args.get('catalogue')
        jsonify(accession)
        sketch_dir = f'/signatures/{catalogue}'
        pattern = os.path.join(sketch_dir, f"{accession}.fna.sig")
        matching_files = glob.glob(pattern)

        if not matching_files:
            return jsonify({'error': f'No .sig file found for accession: {accession}'}), 404

        sig_file_path = matching_files[0]  # Use the first match

        try:
            with open(sig_file_path, 'r') as f:
                signature_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return jsonify({'error': 'Could not read or parse the signature file'}), 400

        try:
            # Call getacc with the loaded signature
            mastiff_df = getacc(signature_data, app.config)
        except SearchError as e:
            return jsonify({'error': str(e)}), 500

        print(f"SANITY CHECK: {len(mastiff_df)}")

        acc_t = tuple(mastiff_df.SRA_accession.tolist())

        meta_list = (
            'bioproject', 'assay_type', 'collection_date_sam',
            'geo_loc_name_country_calc', 'organism', 'lat_lon'
        )

        result_list = getmongo(acc_t, meta_list, app.config)
        print(f"Metadata for {len(result_list)} acc returned.")

        mastiff_dict = mastiff_df.to_dict('records')

        for r in result_list:
            for m in mastiff_dict:
                if r['acc'] == m['SRA_accession']:
                    r['containment'] = round(m['containment'], 2)
                    r['cANI'] = round(m['cANI'], 2)
                    break

        return jsonify(result_list)

    return render_template('index.html')


# def home():
#     print('NEW METHOD BEING USED')
#     if request.method == 'POST':
#         # Read signature data from file instead of the request
#         with open('MGYG000304657.fna.sig', 'r') as f:
#             signature_data = f.read()
#
#         # If the .sig file is JSON (e.g. Sourmash signature), parse it
#         try:
#             signatures = json.loads(signature_data)
#         except json.JSONDecodeError:
#             return jsonify({'error': 'Invalid signature file format'}), 400
#
#         # Now pass to getacc
#         try:
#             mastiff_df = getacc(signatures, app.config)
#         except SearchError as e:
#             return jsonify({'error': str(e)}), 500
#
#         # Log length
#         print(f"SANITY CHECK: {len(mastiff_df)}")
#
#         acc_t = tuple(mastiff_df.SRA_accession.tolist())
#
#         # Metadata fields to query
#         meta_list = (
#             'bioproject', 'assay_type', 'collection_date_sam',
#             'geo_loc_name_country_calc', 'organism', 'lat_lon'
#         )
#
#         # Query metadata
#         result_list = getmongo(acc_t, meta_list, app.config)
#         print(f"Metadata for {len(result_list)} acc returned.")
#         mastiff_dict = mastiff_df.to_dict('records')
#
#         for r in result_list:
#             for m in mastiff_dict:
#                 if r['acc'] == m['SRA_accession']:
#                     r['containment'] = round(m['containment'], 2)
#                     r['cANI'] = round(m['cANI'], 2)
#                     break
#
#         return jsonify(result_list)
#
#     return render_template('index.html')



@app.route('/', methods=['GET', "POST"])
@app.route('/home', methods=['GET', "POST"])
def home():
    if request.method == 'POST':
        # get signatures from fetch/promise API clientside
        form_data = request.get_json()
        # print('HERE IS FORM DATA')
        # print(form_data)

        # get acc from mastiff (imported from acc.py)
        signatures = form_data['signatures']
        try:
            mastiff_df = getacc(signatures, app.config)
        except SearchError as e:
            return e.args

        # log a statement
        print(f"SANITY CHECK: {len(mastiff_df)}")

        # return mastiff_df.to_json(orient='records')  # return acc results to client

        acc_t = tuple(mastiff_df.SRA_accession.tolist())

        # for 'basic' query, override metadata form with selected categories
        meta_list = ('bioproject', 'assay_type',
                     'collection_date_sam', 'geo_loc_name_country_calc', 'organism', 'lat_lon')

        # get metadata from mongodb (imported from mongoquery.py)
        result_list = getmongo(acc_t, meta_list, app.config)
        print(f"Metadata for {len(result_list)} acc returned.")
        mastiff_dict = mastiff_df.to_dict('records')

        for r in result_list:
            for m in mastiff_dict:
                if r['acc'] == m['SRA_accession']:
                    r['containment'] = round(m['containment'], 2)
                    r['cANI'] = round(m['cANI'], 2)
                    break

        return jsonify(result_list)  # return metadata results to client
    return render_template('index.html')


@app.route('/advanced', methods=['GET', "POST"])
def advanced():
    if request.method == 'POST':
        # get signatures from fetch/promise API clientside
        form_data = request.get_json()
        # print(f"Form JSON is {sys.getsizeof(form_data)} bytes.")

        # get acc from mastiff (imported from acc.py)
        signatures = form_data['signatures']
        try:
            mastiff_df = getacc(signatures, app.config)
        except SearchError as e:
            return e.args

        acc_t = tuple(mastiff_df.SRA_accession.tolist())

        # get metadata from mongodb (imported from mongoquery.py)
        meta_dic = form_data['metadata']
        meta_list = tuple([
                          key for key, value in meta_dic.items() if value])

        result_list = getmongo(acc_t, meta_list, app.config)
        print(f"Metadata for {len(result_list)} acc returned.")
        mastiff_dict = mastiff_df.to_dict('records')

        for r in result_list:
            for m in mastiff_dict:
                if r['acc'] == m['SRA_accession']:
                    r['containment'] = round(m['containment'], 2)
                    r['cANI'] = round(m['cANI'], 2)
                    break

        return jsonify(result_list)  # return metadata results to client
    return render_template('advanced.html')


@app.route('/about', methods=['GET', "POST"])
def metadata():
    return render_template('about.html')

@app.route('/contact', methods=['GET', "POST"])
def contact():
    return render_template('contact.html')

@app.route('/examples', methods=['GET', "POST"])
def examples():
    # note, fetch call sends to '/' route to return 'simple search' results
    return render_template('examples.html')


# in production this changes:
#
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)
