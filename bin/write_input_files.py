#!/usr/bin/env python3
'''
Author:     Connor Natzke
Date:       Mar 2021
Revision:   Mar 2021
Purpose:    Generates input files for PEGASUS workflows
'''
import argparse
import os

def write_multipole_file(element, isotope, gamma_1, gamma_2, dir):
    file_contents = (
        f'{float(gamma_2)} {float(gamma_2)} 2 0 0\n'
        f'{float(gamma_1) + float(gamma_2)} {float(gamma_1)} 2 0 0\n'
    )
    file_name = f'{dir}/Multipole_z{int(element) + 1}.a{isotope}'
    file = open(file_name, "w")
    file.write(file_contents)
    file.close()


def write_decay_file(element, isotope, gamma_1, gamma_2, dir):
    file_contents = (
        '#  Excitation  Halflife    Mode    Daughter    Ex  Intensity   Q\n'
        'P  0.000000    1.0000e+02\n'
        '   BetaMinus   0.0000  1.0000e+00\n'
        f'   BetaMinus   {float(gamma_1) + float(gamma_2)}    1.0000e+00  10.0\n'
    )
    file_name = f'{dir}/z{element}.a{isotope}'
    file = open(file_name, "w")
    file.write(file_contents)
    file.close()


def write_evap_file(element, isotope, gamma_1, gamma_2, dir):
    file_contents = (
        f'{float(gamma_2)} {float(gamma_2)} 100.0 2+ 1.0e-12 2.00 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0\n'
        f'{float(gamma_2) + float(gamma_1)} {float(gamma_1)} 100.0 2+ 1.0e-12 2.00 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0\n'
    )
    file_name = f'{dir}/z{int(element) + 1}.a{isotope}'
    file = open(file_name, "w")
    file.write(file_contents)
    file.close()


def main():

    parser = argparse.ArgumentParser(
        description='Script to prepare files for OSG GGAC simulations')

    parser.add_argument('-z', dest='z', required=True,
                        help="Element (atomic) number")
    parser.add_argument('-a', dest='a', required=True,
                        help="Isotope number")
    parser.add_argument('-g1', dest='g1', required=True,
                        help="First gamma energy in cascade [keV]")
    parser.add_argument('-g2', dest='g2', required=True,
                        help="Second gamma energy in cascade [keV]")

    args, unknown = parser.parse_known_args()
    input_dir = "/home/cnatzke/TRIUMF/GammaGammaSurface145mm/Workflows/inputs"

    if os.path.isdir(input_dir):
        try:
            write_multipole_file(args.z, args.a, args.g1, args.g2, input_dir)
            write_decay_file(args.z, args.a, args.g1, args.g2, input_dir)
            write_evap_file(args.z, args.a, args.g1, args.g2, input_dir)
        except OSError as error:
            caught_error = True
            print(error)



if __name__ == "__main__":
    main()
