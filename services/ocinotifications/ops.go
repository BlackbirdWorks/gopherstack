package ocinotifications

func buildOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateTopic: (*Handler).createTopic, opGetTopic: (*Handler).getTopic,
		opListTopics: (*Handler).listTopics, opUpdateTopic: (*Handler).updateTopic,
		opDeleteTopic: (*Handler).deleteTopic, opChangeTopicComp: (*Handler).changeTopicCompartment,
		opPublishMessage: (*Handler).publishMessage,
		opCreateSub:      (*Handler).createSubscription, opGetSub: (*Handler).getSubscription,
		opListSubs: (*Handler).listSubscriptions, opUpdateSub: (*Handler).updateSubscription,
		opDeleteSub: (*Handler).deleteSubscription, opChangeSubComp: (*Handler).changeSubscriptionCompartment,
		opGetConfirmSub: (*Handler).confirmSubscription, opGetUnsubscription: (*Handler).unsubscribe,
		opResendConfirm: (*Handler).resendConfirmation,
	}
}
