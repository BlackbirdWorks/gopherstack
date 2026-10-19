package app

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	budgetsbackend "github.com/blackbirdworks/gopherstack/services/budgets"
	cebackend "github.com/blackbirdworks/gopherstack/services/ce"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	pricingbackend "github.com/blackbirdworks/gopherstack/services/pricing"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

const (
	budgetsSender = "no-reply-aws@amazon.com"
	dateLayout    = "2006-01-02"
)

var (
	errBudgetsNoBackend = errors.New("backend is not running")
	errBudgetsAction    = errors.New("unsupported budget action")
)

type budgetsCESpend struct{ ce *cebackend.InMemoryBackend }

func (s budgetsCESpend) Spend(start, end time.Time, filters map[string][]string, metric string) (float64, bool) {
	return s.ce.LedgerSpend(start.Format(dateLayout), end.Format(dateLayout), filters, metric), true
}

type budgetsNotifier struct {
	sns *snsbackend.InMemoryBackend
	ses *sesbackend.InMemoryBackend
}

func (n *budgetsNotifier) Notify(_ context.Context, d budgetsbackend.Delivery) error {
	var errs []error

	for _, s := range d.Subscribers {
		switch s.SubscriptionType {
		case "SNS":
			if n.sns == nil {
				errs = append(errs, fmt.Errorf("sns: %w", errBudgetsNoBackend))

				continue
			}

			if _, err := n.sns.Publish(s.Address, d.Message, d.Subject, "", nil); err != nil {
				errs = append(errs, fmt.Errorf("sns publish %s: %w", s.Address, err))
			}
		case "EMAIL":
			errs = append(errs, n.email(s.Address, d))
		}
	}

	return errors.Join(errs...)
}

func (n *budgetsNotifier) email(to string, d budgetsbackend.Delivery) error {
	if n.ses == nil {
		return fmt.Errorf("ses: %w", errBudgetsNoBackend)
	}

	if _, err := mail.ParseAddress(to); err != nil {
		return fmt.Errorf("recipient %q: %w", to, err)
	}

	if err := n.ses.VerifyEmailIdentity(budgetsSender); err != nil {
		return fmt.Errorf("verifying sender: %w", err)
	}

	_, err := n.ses.SendEmail(sesbackend.SendEmailInput{
		From: budgetsSender, To: []string{to}, Subject: d.Subject, BodyText: d.Message,
	})
	if err != nil {
		return fmt.Errorf("ses send to %s: %w", to, err)
	}

	return nil
}

type budgetsExecutor struct {
	iam *iambackend.Handler
	org *organizationsbackend.Handler
	ssm *ssmbackend.Handler
}

func (e *budgetsExecutor) Run(ctx context.Context, _ string, a budgetsbackend.Action, reverse bool) error {
	if a.Definition == nil {
		return fmt.Errorf("%w: missing definition", errBudgetsAction)
	}

	switch a.ActionType {
	case "APPLY_IAM_POLICY":
		return e.runIAM(a.Definition.IamActionDefinition, reverse)
	case "APPLY_SCP_POLICY":
		return e.runSCP(a.Definition.ScpActionDefinition, reverse)
	case "RUN_SSM_DOCUMENTS":
		return e.runSSM(ctx, a.Definition.SsmActionDefinition, reverse)
	default:
		return fmt.Errorf("%w: %s", errBudgetsAction, a.ActionType)
	}
}

func (e *budgetsExecutor) runIAM(d *budgetsbackend.IamActionDefinition, reverse bool) error {
	if e.iam == nil || d == nil {
		return fmt.Errorf("iam: %w", errBudgetsNoBackend)
	}

	b := e.iam.Backend

	type op struct {
		attach, detach func(name, policy string) error
		names          []string
	}

	for _, o := range []op{
		{b.AttachRolePolicy, b.DetachRolePolicy, d.Roles},
		{b.AttachUserPolicy, b.DetachUserPolicy, d.Users},
		{b.AttachGroupPolicy, b.DetachGroupPolicy, d.Groups},
	} {
		for _, name := range o.names {
			fn := o.attach
			if reverse {
				fn = o.detach
			}

			if err := fn(name, d.PolicyArn); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	}

	return nil
}

func (e *budgetsExecutor) runSCP(d *budgetsbackend.ScpActionDefinition, reverse bool) error {
	if e.org == nil || d == nil {
		return fmt.Errorf("organizations: %w", errBudgetsNoBackend)
	}

	for _, target := range d.TargetIDs {
		fn := e.org.Backend.AttachPolicy
		if reverse {
			fn = e.org.Backend.DetachPolicy
		}

		if err := fn(d.PolicyID, target); err != nil {
			return fmt.Errorf("%s: %w", target, err)
		}
	}

	return nil
}

func (e *budgetsExecutor) runSSM(ctx context.Context, d *budgetsbackend.SsmActionDefinition, reverse bool) error {
	if e.ssm == nil || d == nil {
		return fmt.Errorf("ssm: %w", errBudgetsNoBackend)
	}

	doc := ssmDocument(d.ActionSubType, reverse)

	ctx = ssmbackend.WithRegion(ctx, d.Region)

	for _, id := range d.InstanceIDs {
		_, err := e.ssm.Backend.StartAutomationExecution(ctx, &ssmbackend.StartAutomationExecutionInput{
			DocumentName: doc, Parameters: map[string][]string{"InstanceId": {id}},
		})
		if err != nil {
			return fmt.Errorf("%s on %s: %w", doc, id, err)
		}
	}

	return nil
}

// wireBudgets connects Budgets to Cost Explorer spend, SNS/SES alert delivery and IAM/Organizations/SSM actions.
func wireBudgets(byName map[string]service.Registerable) {
	h, ok := byName["Budgets"].(*budgetsbackend.Handler)
	if !ok {
		return
	}

	if ceH, ceOK := byName["Ce"].(*cebackend.Handler); ceOK && ceH.Backend != nil {
		h.Backend.SetSpendSource(budgetsCESpend{ce: ceH.Backend})
	}

	n := &budgetsNotifier{}

	if snsH, snsOK := byName["SNS"].(*snsbackend.Handler); snsOK {
		n.sns, _ = snsH.Backend.(*snsbackend.InMemoryBackend)
	}

	if sesH, sesOK := byName["SES"].(*sesbackend.Handler); sesOK {
		n.ses, _ = sesH.Backend.(*sesbackend.InMemoryBackend)
	}

	h.Backend.SetNotifier(n)

	x := &budgetsExecutor{}
	x.iam, _ = byName["IAM"].(*iambackend.Handler)
	x.org, _ = byName["Organizations"].(*organizationsbackend.Handler)
	x.ssm, _ = byName["SSM"].(*ssmbackend.Handler)
	h.Backend.SetActionExecutor(x)
}

// registerPricingEndpoints serves the price list files that GetPriceListFileUrl points at.
func registerPricingEndpoints(e *echo.Echo, services []service.Registerable) {
	for _, svc := range services {
		if h, ok := svc.(*pricingbackend.Handler); ok {
			e.GET(pricingbackend.FilePathPattern, h.ServePriceListFile)
		}
	}
}

func ssmDocument(subType string, reverse bool) string {
	pair := map[string][2]string{
		"STOP_EC2_INSTANCES": {"AWS-StopEC2Instance", "AWS-StartEC2Instance"},
		"STOP_RDS_INSTANCES": {"AWS-StopRdsInstance", "AWS-StartRdsInstance"},
	}[subType]
	if reverse {
		return pair[1]
	}

	return pair[0]
}
