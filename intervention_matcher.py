#!/usr/bin/env python3
"""
ToxEcology Intervention Matcher v1.0
Automatically matches patients with high exposure source scores to relevant interventions
Creates intervention_assignments records for top 10 interventions per patient
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from pyairtable import Table, Base
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Airtable Configuration
AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')
BASE_ID = 'appqeOTy9LTG6Frg7'  # ToxEcology Scoring Base

# Table names
EXPOSURE_SOURCE_SCORES_TABLE = 'exposure_source_scores'
INTERVENTIONS_TABLE = 'interventions'
INTERVENTION_ASSIGNMENTS_TABLE = 'intervention_assignments'
PATIENTS_TABLE = 'patients'
REPORTS_TABLE = 'reports'
SURVEYS_TABLE = 'exposure_surveys'

# Scoring thresholds
HIGH_SCORE_THRESHOLD = 7.0  # Scores >= 7.0 trigger interventions
MODERATE_SCORE_THRESHOLD = 5.0  # Scores 5-7 may get lower-priority interventions
MAX_INTERVENTIONS_PER_PATIENT = 10  # Limit to top 10 interventions

# Source category matching rules
SOURCE_CATEGORY_KEYWORDS = {
    # Arsenic sources
    'rice': ['rice', 'brown rice', 'rice servings'],
    'well_water': ['well water', 'unfiltered water', 'private well'],
    'produce_arsenic': ['leafy greens', 'spinach', 'kale'],
    'pressure_treated_wood': ['deck', 'playground', 'treated wood'],
    
    # Mercury sources
    'high_mercury_fish': ['tuna', 'swordfish', 'king mackerel', 'fish servings'],
    'dental_amalgam': ['amalgam', 'silver fillings', 'dental'],
    
    # Lead sources
    'pre_1978_home': ['home built', 'old home', 'lead paint'],
    'lead_pipes': ['lead pipes', 'old pipes', 'lead water'],
    'lead_cookware': ['ceramic', 'pottery', 'glazed'],
    'occupational_lead': ['construction', 'battery', 'welding'],
    
    # Cadmium sources
    'smoking': ['smoking', 'cigarettes', 'tobacco'],
    'shellfish': ['oysters', 'mussels', 'scallops', 'shellfish'],
    
    # PFAS sources
    'nonstick_cookware': ['nonstick', 'teflon', 'cookware'],
    'pfas_water': ['pfas water', 'contaminated water'],
    'fast_food': ['fast food', 'takeout', 'food packaging'],
    
    # Mycotoxin - Food sources
    'coffee': ['coffee'],
    'nuts': ['nuts', 'peanuts', 'peanut butter'],
    'grains': ['corn', 'wheat', 'grains'],
    
    # Mycotoxin - Water Damage sources
    'visible_mold': ['mold', 'water damage', 'musty'],
    'hvac': ['hvac', 'ducts', 'air conditioning'],
    'humidity': ['humidity', 'basement', 'damp'],
    
    # VOC sources
    'cleaning_products': ['cleaning', 'cleaners', 'disinfectants'],
    'new_furniture': ['new furniture', 'new mattress', 'off-gassing'],
    'air_fresheners': ['air freshener', 'scented candles', 'fragrance'],
    
    # Pesticide sources
    'produce_pesticides': ['produce', 'dirty dozen', 'non-organic'],
    'lawn_pesticides': ['lawn', 'herbicide', 'weed killer'],
    
    # Phthalate sources
    'personal_care': ['personal care', 'fragrance', 'perfume', 'cologne'],
    'plastic_food': ['plastic', 'food storage', 'microwave'],
}


class InterventionMatcher:
    """Matches patients to interventions based on exposure source scores"""
    
    def __init__(self):
        if not AIRTABLE_API_KEY:
            raise ValueError("AIRTABLE_API_KEY environment variable not set")
        
        self.api = Api(AIRTABLE_API_KEY)
        self.base = self.api.base(BASE_ID)
        
        # Load all interventions into memory for faster matching
        self.interventions = self._load_interventions()
        logger.info(f"Loaded {len(self.interventions)} interventions")
    
    def _load_interventions(self) -> List[Dict]:
        """Load all interventions from Airtable"""
        interventions_table = self.base.table(INTERVENTIONS_TABLE)
        records = interventions_table.all()
        
        interventions = []
        for record in records:
            fields = record['fields']
            interventions.append({
                'record_id': record['id'],
                'intervention_id': fields.get('intervention_id'),
                'domain': fields.get('domain'),
                'source_category': fields.get('source_category', ''),
                'intervention_name': fields.get('intervention_name'),
                'priority_rank': fields.get('priority_rank', 5),
                'difficulty_level': fields.get('difficulty_level', 'Moderate'),
                'expected_reduction_percent': fields.get('expected_reduction_percent', 0),
                'estimated_cost': fields.get('estimated_cost', '$51-200'),
            })
        
        return interventions
    
    def _get_unprocessed_scores(self) -> List[Dict]:
        """Get exposure source scores that haven't been matched to interventions yet"""
        scores_table = self.base.table(EXPOSURE_SOURCE_SCORES_TABLE)
        
        # Get all scores, then filter in Python for those without assignments
        # (Airtable API doesn't support complex joins easily)
        all_scores = scores_table.all()
        
        # Check which ones already have intervention assignments
        assignments_table = self.base.table(INTERVENTION_ASSIGNMENTS_TABLE)
        existing_assignments = assignments_table.all()
        processed_report_ids = {a['fields'].get('report_id') for a in existing_assignments if 'report_id' in a['fields']}
        
        unprocessed = []
        for score_record in all_scores:
            fields = score_record['fields']
            report_id = fields.get('report_id')
            
            # Process if: has report_id AND not already processed
            if report_id and report_id not in processed_report_ids:
                unprocessed.append({
                    'record_id': score_record['id'],
                    'patient_id': fields.get('patient_id'),
                    'report_id': report_id,
                    'survey_id': fields.get('survey_id'),
                    'scores': {
                        'arsenic': fields.get('arsenic_source_score', 0),
                        'mercury': fields.get('mercury_source_score', 0),
                        'lead': fields.get('lead_source_score', 0),
                        'cadmium': fields.get('cadmium_source_score', 0),
                        'pfas': fields.get('pfas_source_score', 0),
                        'phthalates': fields.get('phthalates_source_score', 0),
                        'pesticides': fields.get('pesticides_source_score', 0),
                        'vocs': fields.get('vocs_source_score', 0),
                        'parabens': fields.get('parabens_source_score', 0),
                        'mycotoxins_food': fields.get('mycotoxins_food_score', 0),
                        'mycotoxins_water_damage': fields.get('mycotoxins_water_damage_score', 0),
                    },
                    'primary_sources': {
                        'arsenic': fields.get('arsenic_primary_source', ''),
                        'mercury': fields.get('mercury_primary_source', ''),
                        'lead': fields.get('lead_primary_source', ''),
                        'cadmium': fields.get('cadmium_primary_source', ''),
                        'pfas': fields.get('pfas_primary_source', ''),
                        'phthalates': fields.get('phthalates_primary_source', ''),
                        'pesticides': fields.get('pesticides_primary_source', ''),
                        'vocs': fields.get('vocs_primary_source', ''),
                        'parabens': fields.get('parabens_primary_source', ''),
                        'mycotoxins_food': fields.get('mycotoxins_food_primary_source', ''),
                        'mycotoxins_water_damage': fields.get('mycotoxins_water_damage_primary_source', ''),
                    },
                    'confidence': {
                        'arsenic': fields.get('arsenic_confidence', 'low'),
                        'mercury': fields.get('mercury_confidence', 'low'),
                        'lead': fields.get('lead_confidence', 'low'),
                        'cadmium': fields.get('cadmium_confidence', 'low'),
                        'pfas': fields.get('pfas_confidence', 'low'),
                        'phthalates': fields.get('phthalates_confidence', 'low'),
                        'pesticides': fields.get('pesticides_confidence', 'low'),
                        'vocs': fields.get('vocs_confidence', 'low'),
                    }
                })
        
        return unprocessed
    
    def _match_interventions_for_patient(self, score_data: Dict) -> List[Tuple[Dict, int, str]]:
        """
        Match interventions to a patient based on their exposure scores
        
        Returns: List of (intervention, priority, reason) tuples
        """
        matches = []
        scores = score_data['scores']
        primary_sources = score_data['primary_sources']
        confidence = score_data['confidence']
        
        # Domain mapping for intervention matching
        domain_map = {
            'arsenic': 'Arsenic',
            'mercury': 'Mercury',
            'lead': 'Lead',
            'cadmium': 'Cadmium',
            'pfas': 'PFAS',
            'phthalates': 'Phthalates',
            'pesticides': 'Pesticides',
            'vocs': 'VOCs',
            'parabens': 'Parabens',
            'mycotoxins_food': 'Mycotoxins-Food',
            'mycotoxins_water_damage': 'Mycotoxins-WaterDamage',
        }
        
        # Iterate through each domain score
        for domain_key, domain_name in domain_map.items():
            score = scores.get(domain_key, 0)
            
            # Only match interventions for scores >= 5.0 (moderate+)
            if score < MODERATE_SCORE_THRESHOLD:
                continue
            
            primary_source = primary_sources.get(domain_key, '').lower()
            conf = confidence.get(domain_key, 'low').lower()
            
            # Determine priority based on score and confidence
            if score >= HIGH_SCORE_THRESHOLD:
                if conf == 'high':
                    base_priority = 1
                elif conf == 'moderate':
                    base_priority = 2
                else:
                    base_priority = 3
            else:  # 5.0 <= score < 7.0
                base_priority = 3
            
            # Find matching interventions for this domain
            for intervention in self.interventions:
                if intervention['domain'] not in [domain_name, domain_name.replace('-', ' ')]:
                    continue
                
                # Check if source category matches
                source_category = intervention['source_category'].lower()
                intervention_priority = intervention['priority_rank']
                
                # Calculate match relevance score
                relevance_score = 0
                
                # Exact source category match (highest relevance)
                if source_category and any(keyword in primary_source for keyword in source_category.split()):
                    relevance_score += 100
                
                # Domain match but no specific source (generic intervention)
                elif not source_category:
                    relevance_score += 50
                
                # If no relevance, skip this intervention
                if relevance_score == 0:
                    continue
                
                # Calculate final priority (lower = higher priority)
                # Factors: base_priority (1-3), intervention_priority_rank (1-3), relevance
                final_priority = base_priority + (intervention_priority - 1) - (relevance_score / 50)
                
                # Generate reason string
                reason = f"{domain_name.replace('-', ' ')} score: {score:.1f}/10"
                if conf != 'low':
                    reason += f" ({conf} confidence)"
                if source_category:
                    reason += f" - Source: {source_category.replace('_', ' ').title()}"
                
                matches.append((intervention, final_priority, reason))
        
        # Sort by priority (lower = better) and limit to top 10
        matches.sort(key=lambda x: x[1])
        return matches[:MAX_INTERVENTIONS_PER_PATIENT]
    
    def _create_intervention_assignments(self, patient_id: str, report_id: str, 
                                        matched_interventions: List[Tuple[Dict, int, str]]) -> int:
        """Create intervention assignment records in Airtable"""
        assignments_table = self.base.table(INTERVENTION_ASSIGNMENTS_TABLE)
        
        created_count = 0
        assigned_date = datetime.now().isoformat()
        
        for idx, (intervention, priority, reason) in enumerate(matched_interventions, 1):
            # Determine priority tier (1, 2, or 3) for the assignment
            if idx <= 3:
                priority_tier = 1
            elif idx <= 6:
                priority_tier = 2
            else:
                priority_tier = 3
            
            assignment_data = {
                'patient_id': patient_id,
                'report_id': report_id,
                'intervention_id': intervention['record_id'],
                'assigned_date': assigned_date,
                'priority': priority_tier,
                'status': 'Not Started',
                'assigned_by': 'Automated',
                'practitioner_notes': reason,
            }
            
            try:
                assignments_table.create(assignment_data)
                created_count += 1
                logger.info(f"Created assignment: {intervention['intervention_name']} (Priority {priority_tier})")
            except Exception as e:
                logger.error(f"Failed to create assignment for {intervention['intervention_name']}: {e}")
        
        return created_count
    
    def process_all_patients(self):
        """Main processing function: match and assign interventions for all unprocessed patients"""
        logger.info("Starting intervention matching process...")
        
        # Get all unprocessed exposure scores
        unprocessed_scores = self._get_unprocessed_scores()
        logger.info(f"Found {len(unprocessed_scores)} patients to process")
        
        if not unprocessed_scores:
            logger.info("No new patients to process. Exiting.")
            return
        
        total_assignments_created = 0
        
        for score_data in unprocessed_scores:
            patient_id = score_data['patient_id']
            report_id = score_data['report_id']
            
            logger.info(f"\nProcessing patient: {patient_id}, report: {report_id}")
            
            # Match interventions for this patient
            matched_interventions = self._match_interventions_for_patient(score_data)
            
            if not matched_interventions:
                logger.warning(f"No matching interventions found for patient {patient_id}")
                continue
            
            logger.info(f"Matched {len(matched_interventions)} interventions")
            
            # Create intervention assignments
            created = self._create_intervention_assignments(patient_id, report_id, matched_interventions)
            total_assignments_created += created
            
            logger.info(f"Created {created} intervention assignments for patient {patient_id}")
        
        logger.info(f"\n{'='*60}")
        logger.info(f"PROCESSING COMPLETE")
        logger.info(f"Patients processed: {len(unprocessed_scores)}")
        logger.info(f"Total intervention assignments created: {total_assignments_created}")
        logger.info(f"{'='*60}")


def main():
    """Main entry point"""
    try:
        matcher = InterventionMatcher()
        matcher.process_all_patients()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
