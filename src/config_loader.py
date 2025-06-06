#!/usr/bin/env python3
"""
Configuration Loader Module

This module provides configuration loading utilities for the email scheduler system.
"""

from typing import List

def get_campaign_types_allowing_followups() -> List[str]:
    """
    Returns a list of email types that are eligible for follow-up scheduling.
    
    Returns:
        List of email type strings that can have follow-ups scheduled
    """
    return [
        'birthday',
        'effective_date', 
        'aep',
        'post_window'
    ] 