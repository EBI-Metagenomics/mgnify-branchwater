import glob
import os

import duckdb
import yaml
import urllib3
from flask import Flask, render_template, request, jsonify, g, current_app
import polars as pl
import json

import sentry_sdk

from validators import validate_query
from schemas import MagsQuery

# from schemas import MagsQuery

sentry_sdk.init(
    os.environ.get("SENTRY_DSN"),
    enable_tracing=True,
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
)

from functions import getacc, getmetadata, getduckdb, SearchError


def http_pool():
    if 'pool' not in g:
        g.pool = urllib3.PoolManager()

    return g.pool

def duckdb_client(config):
    if 'duckdb_client' not in g:
        g.duckdb_client = duckdb.connect(database = config['metadata_duckdb'],
                                         read_only=True)

    return g.duckdb_client

def create_app():
    app = Flask(__name__)

    with app.app_context():
        # may not be needed/not yet integrated
        current_app.config['SECRET_KEY'] = 'my-secret-key'

        # Load configuration from config.yaml
        with open('config.yml', 'r') as file:
            config_data = yaml.safe_load(file)

            current_app.config.update(config_data)

            metadata = getmetadata(current_app.config, http_pool())
            current_app.config.metadata = metadata

    return app

app = create_app()  # create flask/app instance

@app.teardown_appcontext
def teardown_http_pool(exception):
    pool = g.pop('pool', None)

    if pool is not None:
        pool.clear()

@app.teardown_appcontext
def teardown_duckdb_client(exception):
    client = g.pop('duckdb_client', None)

    if client is not None:
        client.close()


KSIZE = app.config.get('ksize', 21)
THRESHOLD = app.config.get('threshold', 0.1)
METADATA = app.config.get('metadata', {})
print(f'ksize: {KSIZE}')
print(f'threshold: {THRESHOLD}')


# @app.route('/health', methods=["GET"])
# def check_health():
#     base_url = 'http://index-service'
#     # base_url = 'http://localhost:8083'
#     http = urllib3.PoolManager()
#     r = http.request('GET',
#                      f"{base_url}/health",
#                      headers={'Content-Type': 'application/json'})
#
#     print(f"Health status: {r.status}")
#
#     if r.status != 200:
#         raise SearchError(r.data.decode('utf-8'), r.status)
#     return jsonify({'status': 'ok'}), 200

@app.route('/mags', methods=["POST"])
@validate_query(MagsQuery)
def search_by_mgyg_accession():
    q: MagsQuery = g.query
    # accession = q.accession
    # catalogue = q.catalogue
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
            mastiff_df = getacc(signature_data, app.config, use_precomputed_sketches=True)
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


# define '/' and 'home' route
@app.route('/', methods=['GET', "POST"])
@app.route('/home', methods=['GET', "POST"])
def home():
    if request.method == 'POST':
        # get signatures from fetch/promise API clientside
        form_data = request.get_json()

        # get acc from mastiff (imported from acc.py)
        signatures = form_data['signatures']
        try:
            mastiff_df = getacc(signatures, app.config, http_pool())
        except SearchError as e:
            return e.args

        # for 'basic' query, override metadata form with selected categories
        meta_list = ('bioproject', 'assay_type',
                     'collection_date_sam', 'geo_loc_name_country_calc', 'organism', 'lat_lon')

        # get metadata from duckdb
        result_list = getduckdb(mastiff_df, meta_list, app.config, duckdb_client(app.config)).pl()
        print(f"FIRST RESULT for {result_list[0]}.")
        print(f"Metadata for {len(result_list)} acc returned.")
        with open("public_emg_runs.json") as f:
            accession_data = json.load(f)

        # Convert to Polars Series for faster .is_in checks
        accession_series = pl.Series("acc", accession_data["accessions"])

        result_list = result_list.with_columns(
            pl.col("acc").is_in(accession_series).alias("in_json_file")
        )




        return result_list.fill_null("NP").write_json(None)  # return metadata results to client
    return render_template('index.html', n_datasets=f"{app.config.metadata['n_datasets']:,}")


@app.route('/advanced', methods=['GET', "POST"])
def advanced():
    if request.method == 'POST':
        # get signatures from fetch/promise API clientside
        form_data = request.get_json()
        # print(f"Form JSON is {sys.getsizeof(form_data)} bytes.")

        # get acc from mastiff (imported from acc.py)
        signatures = form_data['signatures']
        try:
            mastiff_df = getacc(signatures, app.config, http_pool())
        except SearchError as e:
            return e.args

        # get metadata from duckdb
        meta_dic = form_data['metadata']
        meta_list = tuple([
                          key for key, value in meta_dic.items() if value])

        result_list = getduckdb(mastiff_df, meta_list, app.config, duckdb_client(app.config)).pl()
        print(f"Metadata for {len(result_list)} acc returned.")


        with open("my_accessions.json") as f:
            accession_data = json.load(f)

        # Convert to Polars Series for faster .is_in checks
        accession_series = pl.Series("acc", accession_data["accessions"])

        result_list = result_list.with_columns(
            pl.col("acc").is_in(accession_series).alias("in_json_file")
        )

        print(f"MGS ALTERED DATA  {result_list[0]}.")
        return result_list.fill_null("NP").write_json(None)  # return metadata results to client
    return render_template('advanced.html')


@app.route('/about', methods=['GET', "POST"])
def metadata():
    return render_template('about.html', n_datasets=f"{app.config.metadata['n_datasets']:,}")

@app.route('/contact', methods=['GET', "POST"])
def contact():
    return render_template('contact.html')

@app.route('/examples', methods=['GET', "POST"])
def examples():
    # note, fetch call sends to '/' route to return 'simple search' results
    return render_template('examples.html', n_datasets=f"{app.config.metadata['n_datasets']:,}")

@app.route('/health', methods=["GET"])
def check_health():
    # base_url = 'http://index-service'
    # base_url = 'http://localhost:8083'
    base_url = app.config.get('index_server', 'https://branchwater-api.jgi.doe.gov')
    http = urllib3.PoolManager()
    r = http.request('GET',
                     f"{base_url}/health",
                     headers={'Content-Type': 'application/json'})

    print(f"Health status: {r.status}")

    if r.status != 200:
        raise SearchError(r.data.decode('utf-8'), r.status)
    return jsonify({'status': 'ok'}), 200


# in production this changes:
#
if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=8000)
