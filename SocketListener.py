# ingest_and_publish.py
import argparse, os, socket, time, csv
from typing import Dict, List, Optional
from dataclasses import dataclass, field

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from models import Driver, Result, CurrentSession, RaceClass, Event, Lap, Base
from db import SessionLocal, init_db

def parseTimeSTR(s: str) -> Optional[float]:
    s = (s or "").strip()
    if not s or s == "00:00.000": return None
    try:
        parts = s.split(":")
        if len(parts) == 3:
            h = int(parts[0]); m = int(parts[1]); sec = float(parts[2])
            return h * 3600 + m * 60 + sec
        elif len(parts) == 2:
            m = int(parts[0]); sec = float(parts[1])
            return m * 60 + sec
        else:
            return float(parts[0])
    except:
        return None
    
#helpers 
def csvFields(line: str) -> List[str]:
    reader = csv.reader([line.rstrip("\r\n")], delimiter=',', quotechar='"', skipinitialspace=False)
    return next(reader)

def fmt(sec: Optional[float]) -> str:
  if sec is None: return ""
  if sec >= 60:
        m = int(sec // 60); s = sec - 60*m
        return f"{m}:{s:06.3f}"
  return f"{sec:.3f}"

@dataclass
class DriverState:
    number: str
    first: str = ""
    last: str = ""
    team: str = ""
    chassis: str = ""
    active: bool = True

@dataclass
class TimingState:
    session_name: str = ""
    class_name: str = ""
    track_name: str = ""
    flag: str = ""
    last_lap: Dict[str, Optional[float]] = field(default_factory=dict)
    best_lap: Dict[str, Optional[float]] = field(default_factory=dict)
    order: List[str] = field(default_factory=list)
    drivers: Dict[str, DriverState] = field(default_factory=dict)


class OrbitsParser:
    def __init__(self):
        self.s = TimingState()

    def parseLine(self, line: str):
        if not line.strip():
            return
        try:
            f = csvFields(line)
        except:
            return
        if not f or not f[0].startswith("$"):
            return
        tag = f[0]

        if tag == "$B" and len(f) >= 3:
            self.s.session_name = f[2].strip('"')

        elif tag == "$C" and len(f) >= 3:
            self.s.class_name = f[2].strip('"')

        elif tag == "$E" and len(f) >= 3 and f[1].strip('"').upper() == "TRACK":
            self.s.track_name = f[2].strip('"')

        elif tag == "$A":
            number = f[1].strip('"');
            if not number: return
            d = self.s.drivers.get(number, DriverState(number))
            d.first = f[4].strip('"') if len(f) > 4 else d.first
            d.last = f[5].strip('"') if len(f) > 5 else d.last
            d.chassis = f[6].strip('"') if len(f) > 6 else d.chassis
            if len(f) > 7:
                try: 
                    d.active = int(f[7]) == 1
                except:
                    pass
            self.s.drivers[number] = d

        elif tag == "$COMP":
            number = f[1].strip('"');
            if not number: return
            d = self.s.drivers.get(number, DriverState(number))
            d.first = f[4].strip('"') if len(f) > 4 else d.first
            d.last = f[5].strip('"') if len(f) > 5 else d.last
            d.chassis = f[6].strip('"') if len(f) > 6 else d.chassis
            d.team = f[7].strip('"') if len(f) > 7 else d.team
            self.s.drivers[number] = d

        elif tag == "$F":
            if len(f) > 5: self.s.flag = f[5].strip('"').strip()

        elif tag == "$G":
            number = f[2].strip('"')
            try: pos = int(f[1])
            except: return

            self.s.order = [c for c in self.s.order if c != number]
            while len(self.s.order) < pos - 1: self.s.order.append("")
            self.s.order.insert(pos - 1, number)
            while self.s.order and self.s.order[-1] is None: self.s.order.pop()
        
        elif tag == "$H" and len(f) >= 5:
            number = f[2].strip('"')
            lap = parseTimeSTR(f[4].strip('"'))
            if lap is not None: self.s.last_lap[number] = lap

        elif tag == "$SP" and len(f) >= 5:
            number = f[2].strip('"')
            lap = parseTimeSTR(f[4].strip('"'))
            if lap is not None: self.s.last_lap[number] = lap

        elif tag == "$SR" and len(f) >= 5:
            number = f[2].strip('"')
            best = parseTimeSTR(f[4].strip('"'))
            if best is not None:
                cur = self.s.best_lap.get(number)
                self.s.best_lap[number] = min(best, cur) if cur is not None else best
        
        elif tag == "$J" and len(f) >= 3:
            number = f[1].strip('"')
            best = parseTimeSTR(f[2].strip('"'))
            if best is not None:
                cur = self.s.best_lap.get(number)
                self.s.best_lap[number] = min(best, cur) if cur is not None else best





        
            
        
