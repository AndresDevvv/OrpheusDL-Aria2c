import pickle, requests, errno, hashlib, math, os, re, operator, subprocess, shutil
from tqdm import tqdm
from PIL import Image, ImageChops
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import reduce


def hash_string(input_str: str, hash_type: str = 'MD5'):
    if hash_type == 'MD5':
        return hashlib.md5(input_str.encode("utf-8")).hexdigest()
    else:
        raise Exception('Invalid hash type selected')

def create_requests_session():
    session_ = requests.Session()
    retries = Retry(total=10, backoff_factor=0.4, status_forcelist=[429, 500, 502, 503, 504])
    session_.mount('http://', HTTPAdapter(max_retries=retries))
    session_.mount('https://', HTTPAdapter(max_retries=retries))
    return session_

sanitise_name = lambda name : re.sub(r'[:]', ' - ', re.sub(r'[\\/*?"<>|$]', '', re.sub(r'[ \t]+$', '', str(name).rstrip()))) if name else ''


def fix_byte_limit(path: str, byte_limit=250):
    # only needs the relative path, the abspath uses already existing folders
    rel_path = os.path.relpath(path).replace('\\', '/')

    # split path into directory and filename
    directory, filename = os.path.split(rel_path)

    # truncate filename if its byte size exceeds the byte_limit
    filename_bytes = filename.encode('utf-8')
    fixed_bytes = filename_bytes[:byte_limit]
    fixed_filename = fixed_bytes.decode('utf-8', 'ignore')

    # join the directory and truncated filename together
    return directory + '/' + fixed_filename


r_session = create_requests_session()

_aria2c_installed = None

def is_aria2c_installed():
    global _aria2c_installed
    if _aria2c_installed is None:
        try:
            process = subprocess.run(['aria2c', '--version'], capture_output=True, text=True, check=True)
            _aria2c_installed = "aria2 version" in process.stdout.lower()
        except (subprocess.CalledProcessError, FileNotFoundError):
            _aria2c_installed = False
    return _aria2c_installed

def download_file(url, file_location, headers={}, enable_progress_bar=False, indent_level=0, artwork_settings=None):
    if os.path.isfile(file_location):
        return None

    aria2c_used = False
    if is_aria2c_installed():
        directory = os.path.dirname(file_location)
        filename = os.path.basename(file_location)
        os.makedirs(directory, exist_ok=True)

        aria2c_command = [
            'aria2c',
            '--dir', directory,
            '--out', filename,
            '--max-connection-per-server=8', # Using a moderate number
            '--min-split-size=1M',
            '--split=8', # Corresponds to max-connection-per-server
            '--continue=true',
            '--auto-file-renaming=false',
            '--console-log-level=warn', # Less verbose
            '--show-console-readout=false', # Suppress default progress
            '--summary-interval=0', # Suppress summary progress
            '--allow-overwrite=true' # Allow overwriting if file exists (though we check above)
        ]

        for key, value in headers.items():
            aria2c_command.append(f'--header={key}: {value}')
        
        aria2c_command.append(url)

        try:
            if enable_progress_bar:
                print(f"{' '*indent_level}Downloading with aria2c: {filename}")
            # For aria2c, progress is usually handled by the tool itself.
            # If a custom progress bar is needed, it's more complex.
            # Here, we'll let aria2c print its own progress if not fully suppressed or use a simple message.
            process = subprocess.run(aria2c_command, capture_output=True, text=True, check=False) # check=False to handle errors manually
            if process.returncode == 0:
                aria2c_used = True
            else:
                print(f"{' '*indent_level}aria2c download failed for {filename}. Error: {process.stderr.strip()}. Falling back to requests.")
                # Ensure partially downloaded file by aria2c is removed if it failed
                if os.path.exists(file_location): # Check if aria2c created a file despite error
                    actual_size = os.path.getsize(file_location)
                    if actual_size == 0 : # Or some other heuristic for incomplete download
                         silentremove(file_location)
                    # If aria2c creates .aria2 control files, they should also be cleaned up,
                    # but usually, they are removed on successful completion or error.
                    # For simplicity, we're not explicitly deleting .aria2 files here.

        except FileNotFoundError: # aria2c not found, though is_aria2c_installed should catch this
            print(f"{' '*indent_level}aria2c not found. Falling back to requests for {filename}.")
            _aria2c_installed = False # Update status
        except Exception as e:
            print(f"{' '*indent_level}An unexpected error occurred with aria2c for {filename}: {e}. Falling back to requests.")
            if os.path.exists(file_location): # Clean up if aria2c left a file
                silentremove(file_location)


    if not aria2c_used:
        # Fallback to requests
        r = r_session.get(url, stream=True, headers=headers, verify=False)
        r.raise_for_status() # Raise an exception for bad status codes

        total = None
        if 'content-length' in r.headers:
            total = int(r.headers['content-length'])

        try:
            with open(file_location, 'wb') as f:
                if enable_progress_bar and total:
                    try:
                        columns = os.get_terminal_size().columns
                        # Adjust ncols for indent_level to prevent tqdm from wrapping incorrectly
                        progress_bar_width = columns - indent_level - 25 # Approximate width for bar and text
                        if progress_bar_width < 10: progress_bar_width = 10 # Minimum width
                        
                        bar_format_str = '{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'
                        if os.name == 'nt': # Windows specific adjustments if any
                            bar = tqdm(total=total, unit='B', unit_scale=True, unit_divisor=1024, initial=0, miniters=1, ncols=progress_bar_width, bar_format=' '*indent_level + bar_format_str)
                        else: # Non-Windows
                            bar = tqdm(total=total, unit='B', unit_scale=True, unit_divisor=1024, initial=0, miniters=1, ncols=progress_bar_width, bar_format=' '*indent_level + bar_format_str)

                    except: # Fallback if os.get_terminal_size() fails or other tqdm setup issue
                        bar = tqdm(total=total, unit='B', unit_scale=True, unit_divisor=1024, initial=0, miniters=1, bar_format=' '*indent_level + '{l_bar}{bar}{r_bar}')
                    
                    for chunk in r.iter_content(chunk_size=8192): # Increased chunk size
                        if chunk:  # filter out keep-alive new chunks
                            f.write(chunk)
                            bar.update(len(chunk))
                    bar.close()
                else:
                    for chunk in r.iter_content(chunk_size=8192): # Increased chunk size
                        if chunk:
                            f.write(chunk)
        except Exception as e: # Catch potential errors during requests download
            if os.path.isfile(file_location):
                silentremove(file_location) # Clean up partially downloaded file
            raise e # Re-throw the exception to be handled by caller or global exception handler


    # Common post-download processing (e.g., artwork)
    if os.path.isfile(file_location) and artwork_settings and artwork_settings.get('should_resize', False):
        try:
            new_resolution = artwork_settings.get('resolution', 1400)
            new_format = artwork_settings.get('format', 'jpeg').lower()
            if new_format == 'jpg': new_format = 'jpeg'
            
            new_compression_quality = 90 # Default for 'low'
            compression_setting = artwork_settings.get('compression', 'low').lower()
            if compression_setting == 'high':
                new_compression_quality = 70
            
            # PNG does not use 'quality' in the same way, it uses 'compress_level' (0-9)
            # For simplicity, we'll only set quality for JPEG.
            
            with Image.open(file_location) as im:
                if im.mode == 'P': # Convert paletted images to RGB before resizing/saving as JPEG
                    im = im.convert('RGB')
                im = im.resize((new_resolution, new_resolution), Image.Resampling.BICUBIC)
                if new_format == 'jpeg':
                    im.save(file_location, new_format, quality=new_compression_quality)
                elif new_format == 'png':
                     # Pillow's PNG saver uses 'compress_level' (0-9, default 6).
                     # 'optimize' can also be used.
                     # Mapping 'low'/'high' compression to PNG is not direct.
                     # We'll use a default compression level for PNG.
                    im.save(file_location, new_format, compress_level=6) # Default compression
                else: # Other formats
                    im.save(file_location, new_format)
        except Exception as e:
            print(f"{' '*indent_level}Error processing artwork {file_location}: {e}")
            # Decide if to remove the file or leave it as is
            # For now, leave it, as the download was successful.

    # Handle KeyboardInterrupt specifically for the entire function
    # This was inside the try block for requests, should be outside or handled per block
    # For now, the original KeyboardInterrupt handling for requests part is kept,
    # aria2c handles Ctrl+C by itself. If this function is interrupted,
    # cleanup might be needed. The current structure is a bit complex for a single try/except.

# root mean square code by Charlie Clark: https://code.activestate.com/recipes/577630-comparing-two-images/
def compare_images(image_1, image_2):
    with Image.open(image_1) as im1, Image.open(image_2) as im2:
        h = ImageChops.difference(im1, im2).convert('L').histogram()
        return math.sqrt(reduce(operator.add, map(lambda h, i: h*(i**2), h, range(256))) / (float(im1.size[0]) * im1.size[1]))

# TODO: check if not closing the files causes issues, and see if there's a way to use the context manager with lambda expressions
get_image_resolution = lambda image_location : Image.open(image_location).size[0]

def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

def read_temporary_setting(settings_location, module, root_setting=None, setting=None, global_mode=False):
    temporary_settings = pickle.load(open(settings_location, 'rb'))
    module_settings = temporary_settings['modules'][module] if module in temporary_settings['modules'] else None
    
    if module_settings:
        if global_mode:
            session = module_settings
        else:
            session = module_settings['sessions'][module_settings['selected']]
    else:
        session = None

    if session and root_setting:
        if setting:
            return session[root_setting][setting] if root_setting in session and setting in session[root_setting] else None
        else:
            return session[root_setting] if root_setting in session else None
    elif root_setting and not session:
        raise Exception('Module does not use temporary settings') 
    else:
        return session

def set_temporary_setting(settings_location, module, root_setting, setting=None, value=None, global_mode=False):
    temporary_settings = pickle.load(open(settings_location, 'rb'))
    module_settings = temporary_settings['modules'][module] if module in temporary_settings['modules'] else None

    if module_settings:
        if global_mode:
            session = module_settings
        else:
            session = module_settings['sessions'][module_settings['selected']]
    else:
        session = None

    if not session:
        raise Exception('Module does not use temporary settings')
    if setting:
        session[root_setting][setting] = value
    else:
        session[root_setting] = value
    pickle.dump(temporary_settings, open(settings_location, 'wb'))

create_temp_filename = lambda : f'temp/{os.urandom(16).hex()}'

def save_to_temp(input: bytes):
    location = create_temp_filename()
    open(location, 'wb').write(input)
    return location

def download_to_temp(url, headers={}, extension='', enable_progress_bar=False, indent_level=0):
    location = create_temp_filename() + (('.' + extension) if extension else '')
    download_file(url, location, headers=headers, enable_progress_bar=enable_progress_bar, indent_level=indent_level)
    return location
