from .db_import import db


class Settings(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(80))
    value = db.Column(db.String(250))    
    __table_args__ = (db.UniqueConstraint('id'), )


class Accounts(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    pub_address = db.Column(db.String(70))
    raw_address = db.Column(db.String(80))
    crypto = db.Column(db.String(20))
    amount = db.Column(db.Numeric(precision=52, scale=26), default=0) 
    last_update = db.Column(db.DateTime, default=db.func.current_timestamp(),
                                        onupdate=db.func.current_timestamp()) 
    status = db.Column(db.String(10))
    type = db.Column(db.String(30))
    __table_args__ = (db.UniqueConstraint('id'), )


class Wallets(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    pub_address = db.Column(db.String(70))
    raw_address = db.Column(db.String(80))
    mnemonic = db.Column(db.String(1200))
    create_time = db.Column(db.DateTime, default=db.func.current_timestamp(),
                                        onupdate=db.func.current_timestamp())
    status = db.Column(db.String(10))
    type = db.Column(db.String(30))
    __table_args__ = (db.UniqueConstraint('id'), )


class PayoutAuthNonce(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    consumer = db.Column(db.String(80), nullable=False)
    key_id = db.Column(db.String(120), nullable=False)
    nonce = db.Column(db.String(200), nullable=False)
    timestamp = db.Column(db.Integer, nullable=False)
    created_at = db.Column(
        db.DateTime,
        default=db.func.current_timestamp(),
        nullable=False,
    )
    __table_args__ = (
        db.UniqueConstraint("consumer", "key_id", "nonce", name="uq_payout_auth_nonce"),
        db.Index("ix_payout_auth_nonce_created_at", "created_at"),
    )


class PayoutExecution(db.Model):
    execution_id = db.Column(db.String(80), primary_key=True)
    consumer = db.Column(db.String(80), nullable=False)
    external_id = db.Column(db.String(120), nullable=False)
    request_hash = db.Column(db.String(128), nullable=False)
    sidecar_payload_hash = db.Column(db.String(128), nullable=False)
    state = db.Column(db.String(40), nullable=False)
    state_version = db.Column(db.Integer, nullable=False)
    state_transition_id = db.Column(db.String(80), nullable=False)
    state_updated_at = db.Column(db.String(40), nullable=False)
    lease_owner = db.Column(db.String(120))
    lease_expires_at = db.Column(db.String(40))
    attempt_id = db.Column(db.String(80))
    source_wallet = db.Column(db.String(120), nullable=False)
    jetton_master = db.Column(db.String(160), nullable=False)
    jetton_wallet = db.Column(db.String(160))
    chain_id_or_network_id = db.Column(db.String(40), nullable=False)
    masterchain_seqno = db.Column(db.Integer)
    source_seqno = db.Column(db.Integer)
    valid_until = db.Column(db.String(40))
    canonical_payload_json = db.Column(db.Text, nullable=False)
    signed_boc_ref = db.Column(db.String(240))
    signed_boc_hash = db.Column(db.String(128))
    signed_boc_stored_at = db.Column(db.String(40))
    message_hash = db.Column(db.String(128))
    broadcast_provider = db.Column(db.String(80))
    broadcast_attempted_at = db.Column(db.String(40))
    chain_check_metadata = db.Column(db.Text, nullable=False, default="{}")
    payout_queue = db.Column(db.String(120), nullable=False)
    failure_class = db.Column(db.String(80))
    error_code = db.Column(db.String(120))
    error_message = db.Column(db.Text)
    reconciliation_required = db.Column(db.Boolean, nullable=False, default=False)
    message_hashes_json = db.Column(db.Text, nullable=False, default="[]")
    __table_args__ = (
        db.UniqueConstraint("consumer", "external_id", name="uq_payout_execution_consumer_external"),
        db.Index("ix_payout_execution_state_updated", "state", "state_updated_at"),
        db.Index("ix_payout_execution_reconciliation", "reconciliation_required", "state_updated_at"),
    )


class PayoutCallbackOutbox(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(40), nullable=False)
    payload_json = db.Column(db.Text, nullable=False)
    status = db.Column(db.String(40), nullable=False)
    attempts = db.Column(db.Integer, nullable=False, default=0)
    next_attempt_at = db.Column(db.DateTime)
    claimed_at = db.Column(db.DateTime)
    claim_token = db.Column(db.String(120))
    last_http_status = db.Column(db.Integer)
    last_error = db.Column(db.Text)
    last_response_text = db.Column(db.Text)
    created_at = db.Column(
        db.DateTime,
        default=db.func.current_timestamp(),
        nullable=False,
    )
    updated_at = db.Column(
        db.DateTime,
        default=db.func.current_timestamp(),
        onupdate=db.func.current_timestamp(),
        nullable=False,
    )
    sent_at = db.Column(db.DateTime)
    __table_args__ = (
        db.Index("ix_payout_callback_outbox_status_updated", "status", "updated_at"),
        db.Index("ix_payout_callback_outbox_dispatch_due", "status", "next_attempt_at", "updated_at"),
    )
