"""Django models — the control plane. Platform is the PER-PLATFORM PARTITION:
one row carries everything that varies per platform (budget, cron, rps, queue,
kill-switch, owner). Board is the growing inventory; adding rows scales the
system with zero code change.

STATUS: SKELETON. Requires Django to import; shown here as the schema of the
control plane. Wired into a Django app in build-order step 7.
"""
from __future__ import annotations

try:
    from django.db import models
except Exception:  # keep the package importable without Django installed
    models = None


if models is not None:

    class Platform(models.Model):
        name = models.CharField(max_length=64, unique=True)       # "workday"
        family = models.CharField(max_length=32)                  # one_shot | paged | ...
        task_queue = models.CharField(max_length=32, default="scrape-http")
        schedule_cron = models.CharField(max_length=64, default="0 2 * * *")
        nightly_budget = models.BigIntegerField(default=0)        # requests
        rps_cap = models.FloatField(default=1.0)                  # politeness
        batch_size = models.IntegerField(default=25)              # hypothesis value
        enabled = models.BooleanField(default=True)               # kill-switch
        owner = models.CharField(max_length=16, default="new")    # atlas-kt | new

        def __str__(self):
            return self.name

    class Board(models.Model):
        board_id = models.CharField(max_length=128, unique=True)
        platform = models.ForeignKey(Platform, on_delete=models.CASCADE,
                                     related_name="boards")
        slug = models.CharField(max_length=256)
        url = models.URLField(max_length=1024)
        enabled = models.BooleanField(default=True)
        metadata = models.JSONField(default=dict, blank=True)

        def __str__(self):
            return f"{self.platform_id}:{self.board_id}"
