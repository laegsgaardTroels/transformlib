"""Tools used for organizing transformations of data.

    "You destroy everything you touch, Megatron!"
    "That's because everything I touch is food for my hunger.
    My hunger for power!"
    -Optimus Prime and Megatron

See [1].

    MMMWNWMMMMMMMMMMMMMMWNWMMMMMMMMMMMMMMWNWMMMMMMMMMMMMMMWNWMMM
    MMMNddNMMMMMMMMMMMMMKdOWMMMMMMMMMMMMWOdKMMMMMMMMMMMMMNddNMMM
    MMMWo.cXMMMMMMMMMMMMNc.dNMMMMMMMMMMNd.cNMMMMMMMMMMMMXc.oWMMM
    MMMMk. ;0MMMMMMMMMMMWd..lXMMMMMMMMXl..dWMMMMMMMMMMM0; .kMMMM
    MMMM0'  'kNMMMMMMMMMMO.  ;0MMMMMM0:  .OMMMMMMMMMMNk'  '0MMMM
    MMMMX:   .,cok0XWMMMMX;   'kXXXXk'   ;XMMMMWX0koc,.   :XMMMM
    MMMMWo        ..;cok0Kl     ....     lK0koc;..        oWMMMM
    MMMMMk.             .ll.            .ll.             .kMMMMM
    MMMMM0'     .'..     cO,    ;oo;    ,Oc     ..'.     '0MMMMM
    MMMMMX;     ..'''','.:0c    lKKl    l0:.''''''..     ;XMMMMM
    MMMMMWl     ..    ..':Ox.   .cc.   .xO:'..    ..     lWMMMMM
    MMMMMMx.    .'''''..  lO'          'Ol  ..'''''.    .xMMMMMM
    MMMMMM0'        ..',',o0l         .l0o,','..        '0MMMMMM
    MMMMMMX;              .,ll'      'll,.              ;XMMMMMM
    MMMMMMNd;.    ':c;..     ':;.  .,;'     ..;c:'    .;xNMMMMMM
    MMMMMMMkdx;    'o0X0koc,.. .;::;. ..,cok0XKo'    ;xdkWMMMMMM
    MMMMMMMO,;xl.    .oKWMMWX0c.    .c0XWMMWKo.    .lx;,OMMMMMMM
    MMMMMMMK; .dx,     .lKWMM0,      ,0MMWKo.     ,xd. ;KMMMMMMM
    MMMMMMMNc  .cxc.     .l0O,        ,O0l.     .cxc.  cNMMMMMMM
    MMMMMMMWd.   'dd'      ..          ..      'dd'   .dWMMMMMMM
    MMMMMMMMO.    .lk:                        :xl.    .OMMMMMMMM
    MMMMMMMMK,      ,xo.                    .ox,      ,KMMMMMMMM
    MMMMMMMMNc       .ox;                  ;xo.       cNMMMMMMMM
    MMMMMMMMWd.        ;xl.              .lx;        .dWMMMMMMMM
    MMMMMMMMMK:.        .ox,            ,xd.        .:0MMMMMMMMM
    MMMMMMMMMMWXko:'.     :xc.        .ck:     .':okKWMMMMMMMMMM
    MMMMMMMMMMMMMMWN0xc,.  'dd'      'dd'  .,cx0NWMMMMMMMMMMMMMM
    MMMMMMMMMMMMMMMMMMMWKko:;ox:    :xd;:okKWMMMMMMMMMMMMMMMMMMM
    MMMMMMMMMMMMMMMMMMMMMMMWX00Oc''cO0KXWMMMMMMMMMMMMMMMMMMMMMMM
    MMMMMMMMMMMMMMMMMMMMMMMMMMMMNKKNWMMMMMMMMMMMMMMMMMMMMMMMMMMM

References:
    [1] https://tfwiki.net/wiki/Megatron_(G1)
"""
from .node import Node
from .node import Output
from .node import Input

from .transform import Transform
from .transform import DataFrameTransform
from .transform import transform
from .transform import transform_df

from .pipeline import Pipeline

from .testing import ReusedPySparkTestCase

__all__ = [
    'Node', 'Output', 'Input', 'Transform', 'DataFrameTransform', 'transform',
    'transform_df', 'Pipeline', 'discover_pipeline', 'ReusedPySparkTestCase',
]
