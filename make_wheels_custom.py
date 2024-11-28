import argparse
import logging
import io
import os
import json
import hashlib
import tarfile
import urllib.request
from pathlib import Path
from email.message import EmailMessage
from wheel.wheelfile import WheelFile
from zipfile import ZipFile, ZipInfo, ZIP_DEFLATED
import subprocess


PYTHON_VERSION_TAGS = [
    "py3",

]


GO_PYTHON_PLATFORM_MAPPING = {
    ("darwin", "amd64"): "macosx_10_9_x86_64",
    ("darwin", "arm64"): "macosx_11_0_arm64",
    ("linux", "amd64"): "manylinux2014_x86_64",
    ("linux", "arm"): "manylinux2014_armv7l",
    ("linux", "arm64"): "manylinux2014_aarch64",
    ("windows", "386"): "win32",
    ("windows", "amd64"): "win_amd64",
    ("windows", "arm"): "win_arm32",
    ("windows", "arm64"): "win_arm64",
}


class ReproducibleWheelFile(WheelFile):
    def writestr(self, zinfo_or_arcname, data, *args, **kwargs):
        if isinstance(zinfo_or_arcname, ZipInfo):
            zinfo = zinfo_or_arcname
        else:
            assert isinstance(zinfo_or_arcname, str)
            zinfo = ZipInfo(zinfo_or_arcname)
            zinfo.file_size = len(data)
            zinfo.external_attr = 0o0644 << 16
            if zinfo_or_arcname.endswith(".dist-info/RECORD"):
                zinfo.external_attr = 0o0664 << 16
            elif zinfo_or_arcname.startswith("azsqlcp/azsqlcp_"):
                file_permission_mask = (0o755 & 0xFFFF ) << 16 
                file_type_mask = 1 << 31
                zinfo.external_attr = file_permission_mask | file_type_mask


        zinfo.compress_type = ZIP_DEFLATED
        zinfo.date_time = (1980,1,1,0,0,0)
        zinfo.create_system = 3
        super().writestr(zinfo, data, *args, **kwargs)


def make_message(headers, payload=None):
    msg = EmailMessage()
    for name, value in headers:
        if isinstance(value, list):
            for value_part in value:
                msg[name] = value_part
        else:
            msg[name] = value
    if payload:
        msg.set_payload(payload)
    return msg


def write_wheel_file(filename, contents):
    with ReproducibleWheelFile(filename, 'w') as wheel:
        for member_info, member_source in contents.items():
            wheel.writestr(member_info, bytes(member_source))
    return filename


def write_wheel(out_dir, *, name, version, tag, metadata, description, contents):
    wheel_name = f'{name}-{version}-{tag}.whl'
    dist_info  = f'{name}-{version}.dist-info'
    return write_wheel_file(os.path.join(out_dir, wheel_name), {
        **contents,
        f'{dist_info}/METADATA': make_message([
            ('Metadata-Version', '2.3'),
            ('Name', name),
            ('Version', version),
            *metadata,
        ], description),
        f'{dist_info}/WHEEL': make_message([
            ('Wheel-Version', '1.0'),
            ('Generator', 'python make_wheels_custom.py'),
            ('Root-Is-Purelib', 'false'),
            ('Tag', tag),
        ]),
        f'{dist_info}/entry_points.txt': b'[console_scripts]\nazsqlcp = azsqlcp:__main__.main\n', 
    })




def write_azsql_wheel(out_dir, *, version, platform, binary_name):
    contents = {}
    contents['azsqlcp/__init__.py'] = b''
    contents['azsqlcp/__main__.py'] = f'''\
import os, sys
def main():
    argv = [os.path.join(os.path.dirname(__file__), "{binary_name}"), *sys.argv[1:]]
    if os.name == 'posix':
        print(argv[0], argv)
        # os.execv(argv[0], argv)
        import subprocess; sys.exit(subprocess.call(argv))
    else:
        import subprocess; sys.exit(subprocess.call(argv))
'''.encode('ascii')
    


    # include the binary
    data = (BUILD_PATH / binary_name).read_bytes()
    contents['azsqlcp/' + binary_name] = data


    with open('README.pypi.md') as f:
        description = f.read()

    return write_wheel(out_dir,
        name='azsqlcp',
        version=version,
        tag=f'py3-none-{platform}',
        metadata=[
            ('Summary', 'AZSQLCP is a tool to copy between to databases'),
            ('Description-Content-Type', "'text/markdown'; charset=UTF-8; variant=GFM"),
            ('License-Expression', 'MIT'),
            ('License-File', 'LICENSE'),
            ('Classifier', 'License :: OSI Approved :: MIT License'),
            ('Classifier', 'Development Status :: 4 - Beta'),
            ('Classifier', 'Intended Audience :: Developers'),
            ('Classifier', 'Topic :: Software Development :: Command-line Tools'),
            ('Classifier', 'Programming Language :: Other'),
            ('Classifier', 'Programming Language :: Other Scripting Engines'),
            
            
            # ('Project-URL', 'Homepage, https://ziglang.org'),
            ('Project-URL', 'Source Code, https://github.com/jeff-99/azsqlcp'),
            ('Project-URL', 'Bug Tracker, https://github.com/jeff-99/azsqlcp/issues'),
            ('Requires-Python', '>=3.9'),
        ],
        description=description,
        contents=contents,
    )

BUILD_PATH = Path('./dist/bin')


import os

def build_and_write_wheels(
    outdir='dist/'
):
    Path(outdir).mkdir(exist_ok=True)
    
    VERSION = '0.0.1'
    platform_mapping = GO_PYTHON_PLATFORM_MAPPING

  
    for (GOOS, GOARCH), PYTHON_PLATFORM in platform_mapping.items():
        GO_EXT = '.exe' if GOOS == 'windows' else ''
        binary_name = f'azsqlcp_{VERSION.replace('.', '_')}_{GOOS}_{GOARCH}{GO_EXT}'

        env = os.environ.copy()
        env['CGO_ENABLED'] = '0'
        env['GOOS'] = GOOS
        env['GOARCH'] = GOARCH

        result = subprocess.run(['go', 'build', '-o', BUILD_PATH / binary_name, 'main.go'], check=True, env=env)
        if result.returncode != 0:
            print('Error building binary')
            exit(1)
        
        wheel_path = write_azsql_wheel(outdir,
            version=VERSION,
            platform=PYTHON_PLATFORM,
            binary_name=binary_name
            )
        with open(wheel_path, 'rb') as wheel:
            print(f'  {hashlib.sha256(wheel.read()).hexdigest()} {wheel_path}')




def main():
    logging.getLogger("wheel").setLevel(logging.WARNING)
    build_and_write_wheels()



if __name__ == '__main__':
    main()
