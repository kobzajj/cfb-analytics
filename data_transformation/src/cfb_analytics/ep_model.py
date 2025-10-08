from dataclasses import dataclass
import numpy as np
import pandas as pd

@dataclass
class EPModelStub:
    """Toy EP model; replace with trained GLM/XGB/etc."""
    b0: float = -0.6
    b1: float = -0.7
    b2: float = -0.9
    b3: float = 7.0
    def expected_points(self, down, distance, yardline_100):
        dl = pd.Series(down).fillna(1).clip(1,4)
        dst = pd.Series(distance).fillna(10).clip(lower=0)
        yl = pd.Series(yardline_100).fillna(50).clip(0,100)
        return self.b0 + self.b1*dl + self.b2*np.log1p(dst) + self.b3*((100-yl)/100.0)

def compute_epa(df, model, down_col='down', dist_col='distance', yl_col='yardline_100',
                points_col='points_scored', next_down_col='next_down',
                next_dist_col='next_distance', next_yl_col='next_yardline_100'):
    """EPA = EP_after - EP_before - points_scored_on_play (placeholder)."""
    ep_before = model.expected_points(df[down_col], df[dist_col], df[yl_col])
    ep_after  = model.expected_points(df[next_down_col], df[next_dist_col], df[next_yl_col])
    points = df.get(points_col, pd.Series(0, index=df.index)).fillna(0)
    return ep_after - ep_before - points
