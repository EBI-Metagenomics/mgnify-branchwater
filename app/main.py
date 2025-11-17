import os

import duckdb
import yaml
import urllib3
from flask import Flask, render_template, request, jsonify, g, current_app, Response
import polars as pl
import json
import glob

import sentry_sdk
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


def add_mgnify_flags(result_list: pl.DataFrame, emg_runs_path: str = "/data/emg_runs.json") -> pl.DataFrame:
    """
    Add exists_on_mgnify and is_private columns by joining against emg_runs.json.
    - exists_on_mgnify: True if accession found in emg_runs, else False
    - is_private: Boolean from emg_runs if present, else null
    """
    try:
        print(f"Reading EMG runs from '{emg_runs_path}'")
        with open(emg_runs_path, "r", encoding="utf-8") as f:
            flat = json.load(f)
    except FileNotFoundError:
        print(f"EMG runs file not found at '{emg_runs_path}'. Setting defaults.")
        return result_list.with_columns(
            pl.lit(False).alias("exists_on_mgnify"),
            pl.lit(None, dtype=pl.Boolean).alias("is_private"),
        )
    else:
        if not isinstance(flat, list) or len(flat) == 0:
            print("EMG runs JSON invalid or empty. Setting defaults.")
            return result_list.with_columns(
                pl.lit(False).alias("exists_on_mgnify"),
                pl.lit(None, dtype=pl.Boolean).alias("is_private"),
            )
        # Build lookup DF: accession -> (acc, is_private)
        ena_df = (
            pl.DataFrame(flat)
            .select(
                pl.col("accession").cast(pl.Utf8).str.strip_chars().alias("acc"),
                pl.col("is_private").cast(pl.Boolean),
            )
            .unique(subset=["acc"], keep="last")
        )
        # Normalize acc in results and join
        return (
            result_list
            .with_columns(pl.col("acc").cast(pl.Utf8).str.strip_chars())
            .join(ena_df, on="acc", how="left")  # adds is_private
            .with_columns(
                pl.col("is_private").is_not_null().alias("exists_on_mgnify")
            )
        )


# define '/' and 'home' route
@app.route('/', methods=['GET', "POST"])
@app.route('/home', methods=['GET', "POST"])
def home():
    if request.method == 'POST':
        form_data = request.get_json()

        signatures = form_data['signatures']
        try:
            mastiff_df = getacc(signatures, app.config, http_pool())
        except SearchError as e:
            return e.args

        meta_list = ('bioproject', 'assay_type',
                     'collection_date_sam', 'geo_loc_name_country_calc', 'organism', 'lat_lon')

        result_list = getduckdb(mastiff_df, meta_list, app.config, duckdb_client(app.config)).pl()
        print(f"FIRST RESULT for {result_list[0]}.")
        print(f"Metadata for {len(result_list)} acc returned.")

        # --- Link to EMG runs (exists_on_mgnify + is_private) ---
        result_list = add_mgnify_flags(result_list)

        # NOTE: If you previously relied on fill_null("NP"), that will coerce booleans.
        # Better: only fill nulls in string columns.
        string_cols = [c for c, dt in zip(result_list.columns, result_list.dtypes) if dt == pl.Utf8]
        if string_cols:
            result_list = result_list.with_columns(
                [pl.col(c).fill_null("NP") for c in string_cols]
            )

        return result_list.write_json(None)

    return render_template('index.html', n_datasets=f"{app.config.metadata['n_datasets']:,}")

# @app.route('/', methods=['GET', "POST"])
# @app.route('/home', methods=['GET', "POST"])
# def home():
#     if request.method == 'POST':
#         # get signatures from fetch/promise API clientside
#         form_data = request.get_json()
#
#         # get acc from mastiff (imported from acc.py)
#         signatures = form_data['signatures']
#         try:
#             mastiff_df = getacc(signatures, app.config, http_pool())
#         except SearchError as e:
#             return e.args
#
#         # for 'basic' query, override metadata form with selected categories
#         meta_list = ('bioproject', 'assay_type',
#                      'collection_date_sam', 'geo_loc_name_country_calc', 'organism', 'lat_lon')
#
#         # get metadata from duckdb
#         result_list = getduckdb(mastiff_df, meta_list, app.config, duckdb_client(app.config)).pl()
#         print(f"FIRST RESULT for {result_list[0]}.")
#         print(f"Metadata for {len(result_list)} acc returned.")
#
#         # TODO: complete mag run linkage using this
#         # with open("public_emg_runs.json") as f:
#         #     accession_data = json.load(f)
#         #
#         # # Convert to Polars Series for faster .is_in checks
#         # accession_series = pl.Series("acc", accession_data["accessions"])
#         #
#         # result_list = result_list.with_columns(
#         #     pl.col("acc").is_in(accession_series).alias("in_json_file")
#         # )
#
#         # TODO: complete mag run linkage using flattened ENA file
#         emg_runs_path = "/data/emg_runs.json"
#         try:
#             # current_app.logger.info(f"Reading EMG runs from '{emg_runs_path}' (cwd={os.getcwd()})")
#             print(f"Reading EMG runs from '{emg_runs_path}'")
#             with open(emg_runs_path, "r", encoding="utf-8") as f:
#                 flat = json.load(f)
#             # Basic sanity logging about the raw JSON
#             n_flat = len(flat) if isinstance(flat, list) else 0
#             has_acc = sum(1 for x in flat if isinstance(x, dict) and "ena_accession" in x) if isinstance(flat, list) else 0
#             has_priv = sum(1 for x in flat if isinstance(x, dict) and "is_private" in x) if isinstance(flat, list) else 0
#             # current_app.logger.info(f"emg_runs.json loaded: type={type(flat).__name__}, entries={n_flat}, entries_with_ena_accession={has_acc}, entries_with_is_private={has_priv}")
#             print(f"emg_runs.json loaded: type={type(flat).__name__}, entries={n_flat}, entries_with_ena_accession={has_acc}, entries_with_is_private={has_priv}")
#         except FileNotFoundError:
#             # If the file isn't present, just add defaults and continue
#             # current_app.logger.warning(f"EMG runs file not found at '{emg_runs_path}'. Using default ena_match/is_private. (results={result_list.height})")
#             print(f"EMG runs file not found at '{emg_runs_path}'. Using default ena_match/is_private. (results={result_list.height})")
#             result_list = result_list.with_columns(
#                 pl.lit(False).alias("ena_match"),
#                 pl.lit(None, dtype=pl.Boolean).alias("is_private"),
#             )
#         else:
#             if not isinstance(flat, list) or not flat:
#                 # Empty or invalid => add defaults
#                 # current_app.logger.warning(f"EMG runs JSON invalid or empty (type={type(flat).__name__}, len={0 if not isinstance(flat, list) else len(flat)}). Using defaults.")
#                 print(f"EMG runs JSON invalid or empty (type={type(flat).__name__}, len={0 if not isinstance(flat, list) else len(flat)}). Using defaults.")
#                 result_list = result_list.with_columns(
#                     pl.lit(False).alias("ena_match"),
#                     pl.lit(None, dtype=pl.Boolean).alias("is_private"),
#                 )
#             else:
#                 # Build a small DF from flattened entries and de-dup by accession
#                 ena_df = (
#                     pl.DataFrame(flat)
#                     .select(
#                         pl.col("ena_accession").cast(pl.Utf8).str.strip_chars().alias("acc"),
#                         pl.col("is_private").cast(pl.Boolean),
#                     )
#                     .unique(subset=["acc"], keep="last")  # keep last if duplicates exist
#                 )
#                 try:
#                     print(f"Constructed ena_df: rows={ena_df.height}, cols={ena_df.width}; sample={ena_df.head(3).to_dicts()}")
#                     # current_app.logger.info(
#                     #     f"Constructed ena_df: rows={ena_df.height}, cols={ena_df.width}; sample={ena_df.head(3).to_dicts()}"
#                     # )
#                 except Exception as e:
#                     print(f"Unable to log ena_df sample: {e}")
#                     # current_app.logger.debug(f"Unable to log ena_df sample: {e}")
#
#                 # Normalize acc in results, left-join, then compute ena_match
#                 result_list = (
#                     result_list
#                     .with_columns(pl.col("acc").cast(pl.Utf8).str.strip_chars())
#                     .join(ena_df, on="acc", how="left")  # adds is_private
#                     .with_columns(
#                         pl.col("is_private").is_not_null().alias("ena_match")
#                     )
#                 )
#                 try:
#                     total = result_list.height
#                     matches = result_list.filter(pl.col("ena_match")).height
#                     null_priv = result_list.select(pl.col("is_private").is_null().sum().alias("nulls")).item()
#                     sample_matches = result_list.filter(pl.col("ena_match")).select(["acc", "is_private"]).head(5).to_dicts()
#                     print(f"Join complete: results={total}, ena_match_true={matches}, is_private_nulls={null_priv}, sample_matches={sample_matches}")
#                     # current_app.logger.info(
#                     #     f"Join complete: results={total}, ena_match_true={matches}, is_private_nulls={null_priv}, sample_matches={sample_matches}"
#                     # )
#                 except Exception as e:
#                     print(f"Unable to log join diagnostics: {e}")
#                     # current_app.logger.debug(f"Unable to log join diagnostics: {e}")
#
#
#         return result_list.fill_null("NP").write_json(None)  # return metadata results to client
#     return render_template('index.html', n_datasets=f"{app.config.metadata['n_datasets']:,}")


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
    base_url = 'http://index-service'
    # base_url = 'http://localhost:8083'
    http = urllib3.PoolManager()
    r = http.request('GET',
                     f"{base_url}/health",
                     headers={'Content-Type': 'application/json'})

    print(f"Health status: {r.status}")

    if r.status != 200:
        raise SearchError(r.data.decode('utf-8'), r.status)
    return jsonify({'status': 'ok'}), 200


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
            mastiff_df = getacc(signature_data, app.config, http_pool(), use_precomputed_sketches=True)
        except SearchError as e:
            return jsonify({'error': str(e)}), 500

        print(f"SANITY CHECK: {len(mastiff_df)}")

        # acc_t = tuple(mastiff_df.SRA_accession.tolist())

        meta_list = (
            'bioproject', 'assay_type', 'collection_date_sam',
            'geo_loc_name_country_calc', 'organism', 'lat_lon'
        )

        # result_list = getmongo(acc_t, meta_list, app.config)
        result_list = getduckdb(mastiff_df, meta_list, app.config, duckdb_client(app.config)).pl()
        print(f"Metadata for {len(result_list)} acc returned.")

        # Enrich with EMG flags for MAGs endpoint too
        result_list = add_mgnify_flags(result_list)

        # Only fill nulls in string columns to avoid coercing booleans
        string_cols = [c for c, dt in zip(result_list.columns, result_list.dtypes) if dt == pl.Utf8]
        if string_cols:
            result_list = result_list.with_columns([
                pl.col(c).fill_null("NP") for c in string_cols
            ])

        json_body = result_list.write_json(None)
        return Response(json_body, mimetype='application/json', status=200)


        # return jsonify(result_list)

    return render_template('index.html')


# in production this changes:
#
if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=8000)
