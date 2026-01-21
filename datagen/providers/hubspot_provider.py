"""
HubSpot Mock Data Provider

Generates realistic HubSpot CRM API objects.
Based on: https://developers.hubspot.com/docs/api/crm/contacts
"""

import random
import string
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from faker import Faker


class HubSpotProvider:
    """Generate mock HubSpot CRM data matching API format."""

    LIFECYCLE_STAGES = [
        "subscriber", "lead", "marketingqualifiedlead",
        "salesqualifiedlead", "opportunity", "customer", "evangelist"
    ]

    LEAD_STATUSES = [
        "NEW", "OPEN", "IN_PROGRESS", "OPEN_DEAL", "UNQUALIFIED",
        "ATTEMPTED_TO_CONTACT", "CONNECTED", "BAD_TIMING"
    ]

    ANALYTICS_SOURCES = [
        "ORGANIC_SEARCH", "PAID_SEARCH", "EMAIL_MARKETING",
        "SOCIAL_MEDIA", "REFERRALS", "OTHER_CAMPAIGNS", "DIRECT_TRAFFIC", "OFFLINE"
    ]

    INDUSTRIES = [
        "ACCOUNTING", "APPAREL", "BANKING", "BIOTECHNOLOGY", "CONSTRUCTION",
        "CONSULTING", "CONSUMER_GOODS", "EDUCATION", "ELECTRONICS", "ENERGY",
        "ENTERTAINMENT", "FINANCE", "FOOD_BEVERAGE", "GOVERNMENT", "HEALTHCARE",
        "HOSPITALITY", "INSURANCE", "LEGAL", "MANUFACTURING", "MARKETING",
        "MEDIA", "NONPROFIT", "REAL_ESTATE", "RETAIL", "TECHNOLOGY",
        "TELECOMMUNICATIONS", "TRANSPORTATION", "UTILITIES"
    ]

    COMPANY_TYPES = ["PROSPECT", "PARTNER", "RESELLER", "VENDOR", "OTHER"]

    DEAL_STAGES = [
        {"id": "appointmentscheduled", "label": "Appointment Scheduled", "probability": 0.2},
        {"id": "qualifiedtobuy", "label": "Qualified to Buy", "probability": 0.4},
        {"id": "presentationscheduled", "label": "Presentation Scheduled", "probability": 0.6},
        {"id": "decisionmakerboughtin", "label": "Decision Maker Bought-In", "probability": 0.8},
        {"id": "contractsent", "label": "Contract Sent", "probability": 0.9},
        {"id": "closedwon", "label": "Closed Won", "probability": 1.0},
        {"id": "closedlost", "label": "Closed Lost", "probability": 0.0},
    ]

    DEAL_TYPES = ["newbusiness", "existingbusiness"]
    PRIORITIES = ["low", "medium", "high"]

    def __init__(self, seed: Optional[int] = None):
        """Initialize with optional seed for reproducibility."""
        self.fake = Faker()
        if seed:
            Faker.seed(seed)
            random.seed(seed)

        # ID counters
        self._contact_id = 100000
        self._company_id = 200000
        self._deal_id = 300000
        self._engagement_id = 400000

    def _next_id(self, id_type: str) -> str:
        """Generate next sequential ID as string (HubSpot uses string IDs)."""
        attr = f"_{id_type}_id"
        current = getattr(self, attr)
        setattr(self, attr, current + 1)
        return str(current)

    def _hubspot_timestamp(self, dt: datetime) -> str:
        """Convert datetime to HubSpot timestamp format (ISO 8601 with milliseconds)."""
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    def generate_contact(
        self,
        shared: Optional[Dict] = None,
        company: Optional[Dict] = None,
    ) -> Dict:
        """
        Generate a HubSpot contact object.

        Args:
            shared: Optional shared customer data for cross-source consistency
            company: Optional company to associate with contact
        """
        contact_id = self._next_id("contact")
        created = shared["created_at"] if shared else self.fake.date_time_between(
            start_date="-2y", end_date="-1d"
        )

        email = shared["email"] if shared else self.fake.email()
        first_name = shared["first_name"] if shared else self.fake.first_name()
        last_name = shared["last_name"] if shared else self.fake.last_name()
        phone = shared["phone"] if shared else (self.fake.phone_number() if random.random() > 0.3 else None)

        # Address from shared data or generate new
        if shared and "address" in shared:
            addr = shared["address"]
            address = addr.get("address1")
            city = addr.get("city")
            state = addr.get("province")
            zip_code = addr.get("zip")
            country = addr.get("country", "United States")
        else:
            address = self.fake.street_address() if random.random() > 0.4 else None
            city = self.fake.city() if random.random() > 0.3 else None
            state = self.fake.state() if random.random() > 0.3 else None
            zip_code = self.fake.zipcode() if random.random() > 0.4 else None
            country = "United States" if random.random() > 0.3 else None

        # Lifecycle stage with realistic distribution
        lifecycle_stage = random.choices(
            self.LIFECYCLE_STAGES,
            weights=[15, 25, 15, 15, 10, 15, 5]
        )[0]

        # Analytics data
        page_views = random.randint(0, 100) if random.random() > 0.3 else None
        visits = random.randint(0, 20) if page_views else None

        company_name = None
        associated_company_id = None
        if company:
            company_name = company.get("name")
            associated_company_id = company.get("id")
        elif random.random() > 0.4:
            company_name = self.fake.company()

        properties = {
            "email": email,
            "firstname": first_name,
            "lastname": last_name,
            "phone": phone,
            "mobilephone": self.fake.phone_number() if random.random() > 0.7 else None,
            "company": company_name,
            "jobtitle": self.fake.job() if random.random() > 0.4 else None,
            "lifecyclestage": lifecycle_stage,
            "hs_lead_status": random.choice(self.LEAD_STATUSES) if lifecycle_stage in ["lead", "marketingqualifiedlead", "salesqualifiedlead"] else None,
            "address": address,
            "city": city,
            "state": state,
            "zip": zip_code,
            "country": country,
            "website": self.fake.url() if random.random() > 0.7 else None,
            "hs_analytics_source": random.choice(self.ANALYTICS_SOURCES) if random.random() > 0.3 else None,
            "hs_analytics_first_url": f"/{self.fake.uri_path()}" if random.random() > 0.5 else None,
            "hs_analytics_num_page_views": str(page_views) if page_views else None,
            "hs_analytics_num_visits": str(visits) if visits else None,
            "hs_email_optout": str(random.random() > 0.9).lower(),
            "createdate": self._hubspot_timestamp(created),
            "lastmodifieddate": self._hubspot_timestamp(
                self.fake.date_time_between(start_date=created, end_date="now")
            ),
            "hs_object_id": contact_id,
        }

        return {
            "id": contact_id,
            "properties": properties,
            "email": email,
            "firstname": first_name,
            "lastname": last_name,
            "phone": phone,
            "mobilephone": properties["mobilephone"],
            "company": company_name,
            "jobtitle": properties["jobtitle"],
            "lifecyclestage": lifecycle_stage,
            "hs_lead_status": properties["hs_lead_status"],
            "address": address,
            "city": city,
            "state": state,
            "zip": zip_code,
            "country": country,
            "website": properties["website"],
            "hs_analytics_source": properties["hs_analytics_source"],
            "hs_analytics_first_url": properties["hs_analytics_first_url"],
            "hs_analytics_num_page_views": page_views,
            "hs_analytics_num_visits": visits,
            "hs_email_optout": random.random() > 0.9,
            "createdate": self._hubspot_timestamp(created),
            "lastmodifieddate": properties["lastmodifieddate"],
            "hs_object_id": contact_id,
            "associatedcompanyid": associated_company_id,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_subscription_type": "contact.creation",
        }

    def generate_company(self) -> Dict:
        """Generate a HubSpot company object."""
        company_id = self._next_id("company")
        created = self.fake.date_time_between(start_date="-2y", end_date="-1M")

        name = self.fake.company()
        domain = self.fake.domain_name()

        # Company size and revenue
        num_employees = random.choice([
            None,
            random.randint(1, 10),
            random.randint(11, 50),
            random.randint(51, 200),
            random.randint(201, 1000),
            random.randint(1001, 5000),
            random.randint(5001, 50000),
        ])

        annual_revenue = None
        if num_employees:
            # Rough revenue estimation based on employee count
            base_revenue = num_employees * random.randint(50000, 200000)
            annual_revenue = round(base_revenue, -3)  # Round to nearest thousand

        # Associated counts
        num_contacts = random.randint(0, 20)
        num_deals = random.randint(0, 5) if num_contacts > 0 else 0

        total_revenue = None
        recent_deal_amount = None
        recent_deal_close = None
        if num_deals > 0:
            recent_deal_amount = round(random.uniform(1000, 100000), 2)
            recent_deal_close = self.fake.date_time_between(start_date="-6M", end_date="now")
            total_revenue = round(recent_deal_amount * random.uniform(1, 5), 2)

        properties = {
            "name": name,
            "domain": domain,
            "description": self.fake.catch_phrase() if random.random() > 0.5 else None,
            "industry": random.choice(self.INDUSTRIES) if random.random() > 0.2 else None,
            "type": random.choice(self.COMPANY_TYPES) if random.random() > 0.3 else None,
            "phone": self.fake.phone_number() if random.random() > 0.4 else None,
            "address": self.fake.street_address() if random.random() > 0.5 else None,
            "address2": None,
            "city": self.fake.city() if random.random() > 0.4 else None,
            "state": self.fake.state() if random.random() > 0.4 else None,
            "zip": self.fake.zipcode() if random.random() > 0.5 else None,
            "country": "United States" if random.random() > 0.3 else None,
            "timezone": self.fake.timezone() if random.random() > 0.6 else None,
            "website": f"https://{domain}",
            "linkedin_company_page": f"https://linkedin.com/company/{name.lower().replace(' ', '-')}" if random.random() > 0.5 else None,
            "facebook_company_page": None,
            "twitterhandle": f"@{name.split()[0].lower()}" if random.random() > 0.6 else None,
            "numberofemployees": str(num_employees) if num_employees else None,
            "annualrevenue": str(annual_revenue) if annual_revenue else None,
            "lifecyclestage": random.choice(["lead", "opportunity", "customer"]) if random.random() > 0.3 else None,
            "hs_lead_status": random.choice(self.LEAD_STATUSES) if random.random() > 0.5 else None,
            "hubspot_owner_id": str(random.randint(10000, 99999)) if random.random() > 0.4 else None,
            "createdate": self._hubspot_timestamp(created),
            "lastmodifieddate": self._hubspot_timestamp(
                self.fake.date_time_between(start_date=created, end_date="now")
            ),
            "hs_object_id": company_id,
        }

        return {
            "id": company_id,
            "properties": properties,
            "name": name,
            "domain": domain,
            "description": properties["description"],
            "industry": properties["industry"],
            "type": properties["type"],
            "phone": properties["phone"],
            "address": properties["address"],
            "address2": None,
            "city": properties["city"],
            "state": properties["state"],
            "zip": properties["zip"],
            "country": properties["country"],
            "timezone": properties["timezone"],
            "website": properties["website"],
            "linkedin_company_page": properties["linkedin_company_page"],
            "facebook_company_page": None,
            "twitterhandle": properties["twitterhandle"],
            "numberofemployees": num_employees,
            "annualrevenue": annual_revenue,
            "lifecyclestage": properties["lifecyclestage"],
            "hs_lead_status": properties["hs_lead_status"],
            "hubspot_owner_id": properties["hubspot_owner_id"],
            "notes_last_contacted": self._hubspot_timestamp(
                self.fake.date_time_between(start_date="-30d", end_date="now")
            ) if random.random() > 0.5 else None,
            "notes_last_updated": None,
            "notes_next_activity_date": self._hubspot_timestamp(
                self.fake.date_time_between(start_date="now", end_date="+30d")
            ) if random.random() > 0.6 else None,
            "num_associated_contacts": num_contacts,
            "num_associated_deals": num_deals,
            "recent_deal_amount": recent_deal_amount,
            "recent_deal_close_date": self._hubspot_timestamp(recent_deal_close) if recent_deal_close else None,
            "total_revenue": total_revenue,
            "createdate": self._hubspot_timestamp(created),
            "lastmodifieddate": properties["lastmodifieddate"],
            "hs_object_id": company_id,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_subscription_type": "company.creation",
        }

    def generate_deal(
        self,
        contact: Optional[Dict] = None,
        company: Optional[Dict] = None,
    ) -> Dict:
        """
        Generate a HubSpot deal object.

        Args:
            contact: Optional contact to associate with deal
            company: Optional company to associate with deal
        """
        deal_id = self._next_id("deal")
        created = self.fake.date_time_between(start_date="-6M", end_date="-1d")

        # Select deal stage with realistic distribution
        stage = random.choices(
            self.DEAL_STAGES,
            weights=[15, 15, 15, 15, 15, 15, 10]
        )[0]

        is_closed = stage["id"] in ["closedwon", "closedlost"]
        is_won = stage["id"] == "closedwon"

        # Deal amount
        amount = round(random.uniform(1000, 100000), 2) if random.random() > 0.2 else None

        # Calculate weighted amount
        forecast_amount = None
        if amount:
            forecast_amount = round(amount * stage["probability"], 2)

        # Close date
        if is_closed:
            close_date = self.fake.date_time_between(start_date=created, end_date="now")
        else:
            close_date = self.fake.date_time_between(start_date="now", end_date="+90d")

        # Deal name
        if company:
            deal_name = f"{company['name']} - {self.fake.bs().title()}"
        elif contact:
            deal_name = f"{contact.get('firstname', '')} {contact.get('lastname', '')} - {self.fake.bs().title()}"
        else:
            deal_name = f"{self.fake.company()} - {self.fake.bs().title()}"

        properties = {
            "dealname": deal_name,
            "dealstage": stage["id"],
            "pipeline": "default",
            "amount": str(amount) if amount else None,
            "closedate": self._hubspot_timestamp(close_date),
            "dealtype": random.choice(self.DEAL_TYPES) if random.random() > 0.3 else None,
            "description": self.fake.paragraph() if random.random() > 0.6 else None,
            "hs_priority": random.choice(self.PRIORITIES) if random.random() > 0.4 else None,
            "hs_deal_stage_probability": str(stage["probability"]),
            "hs_forecast_amount": str(forecast_amount) if forecast_amount else None,
            "hs_forecast_probability": str(stage["probability"]),
            "hs_is_closed": str(is_closed).lower(),
            "hs_is_closed_won": str(is_won).lower(),
            "hubspot_owner_id": str(random.randint(10000, 99999)) if random.random() > 0.3 else None,
            "hs_created_by_user_id": str(random.randint(10000, 99999)),
            "createdate": self._hubspot_timestamp(created),
            "lastmodifieddate": self._hubspot_timestamp(
                self.fake.date_time_between(start_date=created, end_date="now")
            ),
            "hs_object_id": deal_id,
        }

        return {
            "id": deal_id,
            "properties": properties,
            "dealname": deal_name,
            "dealstage": stage["id"],
            "pipeline": "default",
            "amount": amount,
            "closedate": self._hubspot_timestamp(close_date),
            "dealtype": properties["dealtype"],
            "description": properties["description"],
            "hs_priority": properties["hs_priority"],
            "hs_deal_stage_probability": stage["probability"],
            "hs_forecast_amount": forecast_amount,
            "hs_forecast_probability": stage["probability"],
            "hs_is_closed": is_closed,
            "hs_is_closed_won": is_won,
            "hubspot_owner_id": properties["hubspot_owner_id"],
            "hs_created_by_user_id": properties["hs_created_by_user_id"],
            "notes_last_contacted": self._hubspot_timestamp(
                self.fake.date_time_between(start_date="-14d", end_date="now")
            ) if random.random() > 0.4 else None,
            "notes_last_updated": None,
            "notes_next_activity_date": self._hubspot_timestamp(
                self.fake.date_time_between(start_date="now", end_date="+14d")
            ) if not is_closed and random.random() > 0.5 else None,
            "num_associated_contacts": 1 if contact else random.randint(0, 3),
            "num_contacted_notes": random.randint(0, 10) if random.random() > 0.5 else None,
            "num_notes": random.randint(0, 20) if random.random() > 0.4 else None,
            "hs_analytics_source": random.choice(self.ANALYTICS_SOURCES) if random.random() > 0.5 else None,
            "hs_analytics_source_data_1": None,
            "hs_analytics_source_data_2": None,
            "hs_lastmodifieddate": properties["lastmodifieddate"],
            "hs_createdate": properties["createdate"],
            "createdate": self._hubspot_timestamp(created),
            "lastmodifieddate": properties["lastmodifieddate"],
            "hs_object_id": deal_id,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_subscription_type": "deal.creation",
        }

    def generate_engagement(
        self,
        contact: Optional[Dict] = None,
        company: Optional[Dict] = None,
        deal: Optional[Dict] = None,
        engagement_type: Optional[str] = None,
    ) -> Dict:
        """Generate a HubSpot engagement (activity) object."""
        engagement_id = self._next_id("engagement")
        created = self.fake.date_time_between(start_date="-3M", end_date="now")

        if engagement_type is None:
            engagement_type = random.choice(["NOTE", "EMAIL", "TASK", "MEETING", "CALL"])

        # Build associations
        associations = {}
        if contact:
            associations["contactIds"] = [contact["id"]]
        if company:
            associations["companyIds"] = [company["id"]]
        if deal:
            associations["dealIds"] = [deal["id"]]

        # Type-specific properties
        if engagement_type == "NOTE":
            body = self.fake.paragraph()
        elif engagement_type == "EMAIL":
            body = f"Subject: {self.fake.sentence()}\n\n{self.fake.paragraph()}"
        elif engagement_type == "TASK":
            body = f"Task: {self.fake.sentence()}"
        elif engagement_type == "MEETING":
            body = f"Meeting notes: {self.fake.paragraph()}"
        elif engagement_type == "CALL":
            body = f"Call summary: {self.fake.sentence()}"
        else:
            body = self.fake.sentence()

        return {
            "id": engagement_id,
            "type": engagement_type,
            "properties": {
                "hs_timestamp": self._hubspot_timestamp(created),
                "hs_body_preview": body[:100] + "..." if len(body) > 100 else body,
            },
            "hs_timestamp": self._hubspot_timestamp(created),
            "hs_created_by": str(random.randint(10000, 99999)),
            "hubspot_owner_id": str(random.randint(10000, 99999)) if random.random() > 0.3 else None,
            "hs_body_preview": body[:100] + "..." if len(body) > 100 else body,
            "hs_body_preview_html": f"<p>{body[:100]}...</p>" if len(body) > 100 else f"<p>{body}</p>",
            "hs_body_preview_is_truncated": len(body) > 100,
            "associations": associations,
            "createdate": self._hubspot_timestamp(created),
            "lastmodifieddate": self._hubspot_timestamp(
                self.fake.date_time_between(start_date=created, end_date="now")
            ),
            "hs_object_id": engagement_id,
            "_webhook_received_at": datetime.now(timezone.utc).isoformat() + "Z",
            "_webhook_subscription_type": f"engagement.{engagement_type.lower()}",
        }
