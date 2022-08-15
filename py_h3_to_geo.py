# -*- coding: utf-8 -*-
"""
Created on Wed May  4 10:49:47 2022

@author: gabriel.ferraz
"""

import sys
import h3


def main(latitude, longitude):  
    results = h3.geo_to_h3(float(latitude), float(longitude), 5)
    print (results)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])

# =============================================================================
# if __name__ == '__main__':
#     main(-23.9500, -50.4479)
# =============================================================================
