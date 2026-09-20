package server

import (
	"heckel.io/ntfy/v2/log"
	"heckel.io/ntfy/v2/metrics"
	"heckel.io/ntfy/v2/util"
)

func (s *Server) execManager() {
	// WARNING: Make sure to only selectively lock with the mutex, and be aware that this
	//          there is no mutex for the entire function.

	// Prune all the things
	s.pruneVisitors()
	s.pruneTokens()
	s.pruneAttachments()
	s.pruneMessages()
	s.pruneAndNotifyWebPushSubscriptions()

	// Message count
	messagesCached, err := s.messageCache.MessagesCount()
	if err != nil {
		log.Tag(tagManager).Err(err).Warn("Cannot get messages count")
	}

	// Remove subscriptions without subscribers
	var emptyTopics, subscribers int
	log.
		Tag(tagManager).
		Timing(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, t := range s.topics {
				subs, lastAccess := t.Stats()
				ev := log.Tag(tagManager).With(t)
				if t.Stale() {
					if ev.IsTrace() {
						ev.Trace("- topic %s: Deleting stale topic (%d subscribers, accessed %s)", t.ID, subs, util.FormatTime(lastAccess))
					}
					emptyTopics++
					delete(s.topics, t.ID)
				} else {
					if ev.IsTrace() {
						ev.Trace("- topic %s: %d subscribers, accessed %s", t.ID, subs, util.FormatTime(lastAccess))
					}
					subscribers += subs
				}
			}
		}).
		Debug("Removed %d empty topic(s)", emptyTopics)

	// Mail stats
	var receivedMailTotal, receivedMailSuccess, receivedMailFailure int64
	if s.smtpServerBackend != nil {
		receivedMailTotal, receivedMailSuccess, receivedMailFailure = s.smtpServerBackend.Counts()
	}
	var sentMailTotal, sentMailSuccess, sentMailFailure int64
	if s.mailer != nil {
		sentMailTotal, sentMailSuccess, sentMailFailure = s.mailer.NotificationCounts()
	}

	// Users
	var usersCount int64
	if s.userManager != nil {
		usersCount, err = s.userManager.UsersCount()
		if err != nil {
			log.Tag(tagManager).Err(err).Warn("Error counting users")
		}
	}

	// Print stats
	s.mu.RLock()
	messagesCount, topicsCount, visitorsCount := s.messages, len(s.topics), len(s.visitors)
	s.mu.RUnlock()

	// Update stats
	s.updateAndWriteStats(messagesCount)

	// Log stats
	log.
		Tag(tagManager).
		Fields(log.Context{
			"messages_published":      messagesCount,
			"messages_cached":         messagesCached,
			"topics_active":           topicsCount,
			"subscribers":             subscribers,
			"visitors":                visitorsCount,
			"users":                   usersCount,
			"emails_received":         receivedMailTotal,
			"emails_received_success": receivedMailSuccess,
			"emails_received_failure": receivedMailFailure,
			"emails_sent":             sentMailTotal,
			"emails_sent_success":     sentMailSuccess,
			"emails_sent_failure":     sentMailFailure,
		}).
		Info("Server stats")
	metrics.MessagesCached.Set(float64(messagesCached))
	metrics.Visitors.Set(float64(visitorsCount))
	metrics.Users.Set(float64(usersCount))
	metrics.Subscribers.Set(float64(subscribers))
	metrics.Topics.Set(float64(topicsCount))
	if s.attachment != nil {
		metrics.AttachmentsTotalSize.Set(float64(s.attachment.Size()))
	}
}

func (s *Server) pruneVisitors() {
	staleVisitors := 0
	log.
		Tag(tagManager).
		Timing(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			for ip, v := range s.visitors {
				if v.Stale() {
					log.Tag(tagManager).With(v).Trace("Deleting stale visitor")
					delete(s.visitors, ip)
					staleVisitors++
				}
			}
		}).
		Field("stale_visitors", staleVisitors).
		Debug("Finished deleting stale visitors")
}

func (s *Server) pruneTokens() {
	if s.userManager != nil {
		log.
			Tag(tagManager).
			Timing(func() {
				if err := s.userManager.RemoveExpiredTokens(); err != nil {
					log.Tag(tagManager).Err(err).Warn("Error expiring user tokens")
				}
				if err := s.userManager.RemoveDeletedUsers(); err != nil {
					log.Tag(tagManager).Err(err).Warn("Error deleting soft-deleted users")
				}
			}).
			Debug("Finished deleting expired tokens and users")
	}
}

func (s *Server) pruneAttachments() {
	if s.attachment == nil {
		return
	}
	// Mark expired attachments as deleted in the DB and remove their files in the same
	// pass. Deletion is by exact message ID, so concurrently published messages (unique
	// IDs) are never affected. The attachment store's sync() loop remains as a safety
	// net for orphans (e.g. files of messages that failed to persist).
	log.
		Tag(tagManager).
		Timing(func() {
			ids, err := s.messageCache.MarkExpiredAttachmentsDeleted(s.config.ManagerBatchSize)
			if err != nil {
				log.Tag(tagManager).Err(err).Warn("Error marking expired attachments as deleted")
			} else if len(ids) > 0 {
				if err := s.attachment.Remove(ids...); err != nil {
					log.Tag(tagManager).Err(err).Warn("Error deleting expired attachment files")
				}
				log.Tag(tagManager).Debug("Deleted %d expired attachment(s)", len(ids))
			} else {
				log.Tag(tagManager).Debug("No expired attachments to delete")
			}
		}).
		Debug("Finished deleting expired attachments")
}

func (s *Server) pruneMessages() {
	// Delete expired message rows and remove their attachment files in the same pass.
	// This also covers messages whose attachments were not yet marked as deleted (e.g.
	// when the batch size was exhausted), so blobs never outlive their message rows.
	log.
		Tag(tagManager).
		Timing(func() {
			attachmentIDs, err := s.messageCache.DeleteExpiredMessages(s.config.ManagerBatchSize)
			if err != nil {
				log.Tag(tagManager).Err(err).Warn("Error deleting expired messages")
			} else if len(attachmentIDs) > 0 && s.attachment != nil {
				if err := s.attachment.Remove(attachmentIDs...); err != nil {
					log.Tag(tagManager).Err(err).Warn("Error deleting attachments of expired messages")
				}
				log.Tag(tagManager).Debug("Deleted expired messages and %d attachment(s)", len(attachmentIDs))
			} else {
				log.Tag(tagManager).Debug("No expired messages to delete")
			}
		}).
		Debug("Finished deleting expired messages")
}
